import os
import re
import time
import base64
import requests
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = os.environ.get("FROM_EMAIL", "Market Intelligence <updates@market-sigma.com>")
MAX_REQUESTS = int(os.environ.get("MAX_REQUESTS", "50"))

sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def normalize_number(number: str) -> str:
    if not number:
        return ""
    number = number.strip().replace(" ", "")
    if not number.startswith("+"):
        return ""
    if not re.fullmatch(r"\+\d{8,15}", number):
        return ""
    return number


def valid_email(email: str) -> bool:
    if not email:
        return False
    return re.fullmatch(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email) is not None


def load_queued_requests():
    resp = (
        sb.table("dispatch_requests")
        .select("*")
        .eq("status", "queued")
        .order("created_at", desc=False)
        .limit(MAX_REQUESTS)
        .execute()
    )
    return resp.data or []


def get_report(report_id: str):
    resp = (
        sb.table("reports")
        .select("*")
        .eq("id", report_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        raise ValueError(f"Report not found: {report_id}")
    return rows[0]


def get_broadcast_file(file_id: str):
    resp = (
        sb.table("broadcast_files")
        .select("*")
        .eq("id", file_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        raise ValueError(f"Broadcast file not found: {file_id}")
    return rows[0]


def get_recipient(recipient_id: str):
    resp = (
        sb.table("recipients")
        .select("*")
        .eq("id", recipient_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        raise ValueError(f"Recipient not found: {recipient_id}")
    return rows[0]


def mark_request(request_id: str, status: str, notes: str = ""):
    payload = {
        "status": status,
        "processed_at": "now()",
        "notes": notes[:2000] if notes else None,
    }
    sb.table("dispatch_requests").update(payload).eq("id", request_id).execute()


def insert_dispatch_log(report_id, recipient, channel, destination, action_type, triggered_by):
    resp = (
        sb.table("dispatch_logs")
        .insert(
            {
                "report_id": report_id,
                "recipient_id": recipient.get("id"),
                "recipient_name": recipient.get("name"),
                "channel": channel,
                "destination": destination,
                "action_type": action_type,
                "status": "pending",
                "triggered_by": triggered_by,
            }
        )
        .execute()
    )
    rows = resp.data or []
    return rows[0]["id"] if rows else None


def update_dispatch_log(log_id, status, response_text):
    if not log_id:
        return
    payload = {
        "status": status,
        "response_text": response_text[:4000] if response_text else None,
        "sent_at": "now()" if status == "sent" else None,
    }
    sb.table("dispatch_logs").update(payload).eq("id", log_id).execute()


def ensure_report_approved(report):
    if (report.get("status") or "").lower() != "approved":
        raise ValueError("Report is not approved. Only approved reports can be dispatched.")


def build_payload_from_report(report):
    report_date = str(report.get("report_date"))
    pdf_url  = report.get("pdf_url")
    # Optional secondary attachment — populated when the upstream pipeline
    # has uploaded an HTML version of the report. Falls back gracefully
    # to PDF-only when not present, so this change is backward compatible.
    html_url = report.get("html_url")

    if html_url:
        caption = (
            f"Doha Bank Market Intelligence\n"
            f"{report_date}\n\n"
            f"Please find the approved market intelligence report attached. "
            f"The PDF is best for desktop or print; the HTML version is "
            f"optimised for mobile reading — open it in any browser."
        )
    else:
        caption = (
            f"Doha Bank Market Intelligence\n"
            f"{report_date}\n\n"
            f"Please find attached the approved market intelligence report."
        )

    return {
        "title": f"Market Intelligence – {report_date}",
        "caption": caption,
        # Primary attachment (PDF) — also used as the WhatsApp link
        "file_url": pdf_url,
        "file_name": f"Market-Intelligence-{report_date}.pdf",
        # Optional secondary attachment (HTML). Both fields must be set
        # for send_email to include it; if either is missing, PDF-only.
        "html_url": html_url,
        "html_file_name": f"Market-Intelligence-{report_date}.html" if html_url else None,
    }


def build_payload_from_broadcast(bf):
    return {
        "title": bf.get("title") or "Broadcast File",
        "caption": bf.get("caption") or "Please find attached the requested file.",
        "file_url": bf.get("file_url"),
        "file_name": bf.get("file_name") or "attachment",
    }


def send_whatsapp(payload_data, recipient, action_type, triggered_by, report_id=None):
    number = normalize_number(recipient.get("phone_number", ""))
    if not number:
        raise ValueError("Invalid WhatsApp phone number")

    payload = {
        "to": number,
        "name": recipient.get("name", "Unknown"),
        "report_date": payload_data.get("title", ""),
        "pdf_url": payload_data.get("file_url"),
        "caption": payload_data.get("caption"),
    }

    log_id = insert_dispatch_log(
        report_id,
        recipient,
        "whatsapp",
        number,
        action_type,
        triggered_by,
    )

    try:
        res = requests.post(MAKE_WEBHOOK_URL, json=payload, timeout=60)
        if res.status_code == 200:
            update_dispatch_log(log_id, "sent", res.text)
            return True, f"WhatsApp sent: {res.text}"
        update_dispatch_log(log_id, "failed", res.text)
        return False, f"WhatsApp failed: {res.status_code} {res.text}"
    except Exception as e:
        update_dispatch_log(log_id, "failed", str(e))
        return False, f"WhatsApp exception: {e}"


def _fetch_attachment(url: str, default_content_type: str = "application/octet-stream"):
    """
    Fetch a file from a URL and return (content_type, base64_content).
    Raises ValueError on non-200 response.
    """
    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        raise ValueError(f"Could not fetch attachment from {url}: HTTP {resp.status_code}")
    content_type = resp.headers.get("content-type", default_content_type)
    return content_type, base64.b64encode(resp.content).decode()


def send_email(payload_data, recipient, action_type, triggered_by, report_id=None):
    email = (recipient.get("email") or "").strip()
    if not valid_email(email):
        raise ValueError("Invalid email address")

    file_url = payload_data.get("file_url")
    file_name = payload_data.get("file_name") or "attachment"
    title = payload_data.get("title") or "Attachment"
    caption = payload_data.get("caption") or "Please find attached the requested file."

    if not file_url:
        raise ValueError("File URL missing")

    # --- Primary attachment (the PDF for report sends, or whatever the
    # broadcast file is for broadcasts) ---
    primary_content_type, primary_b64 = _fetch_attachment(file_url, "application/pdf")
    attachments = [
        {
            "filename": file_name,
            "content": primary_b64,
            "content_type": primary_content_type,
        }
    ]

    # --- Optional secondary attachment: HTML companion of the report ---
    # Best-effort. If the HTML fetch fails for any reason, log a warning
    # and proceed with PDF-only delivery — never block an approved
    # dispatch on a missing companion file.
    html_url = payload_data.get("html_url")
    html_file_name = payload_data.get("html_file_name")
    if html_url and html_file_name:
        try:
            html_content_type, html_b64 = _fetch_attachment(html_url, "text/html; charset=utf-8")
            attachments.append({
                "filename": html_file_name,
                "content": html_b64,
                "content_type": html_content_type,
            })
        except Exception as e:
            print(f"[WARN] HTML companion fetch failed ({e}); sending PDF only")

    payload = {
        "from": FROM_EMAIL,
        "to": [email],
        "subject": title,
        "html": f"""
            <p>Dear {recipient.get('name') or 'Client'},</p>
            <p>{caption}</p>
        """,
        "attachments": attachments,
    }

    log_id = insert_dispatch_log(
        report_id,
        recipient,
        "email",
        email,
        action_type,
        triggered_by,
    )

    try:
        res = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
        if res.status_code in (200, 201):
            update_dispatch_log(log_id, "sent", res.text)
            attach_count = len(attachments)
            return True, (
                f"Email sent with {attach_count} attachment"
                f"{'s' if attach_count != 1 else ''}: {res.text}"
            )
        update_dispatch_log(log_id, "failed", res.text)
        return False, f"Email failed: {res.status_code} {res.text}"
    except Exception as e:
        update_dispatch_log(log_id, "failed", str(e))
        return False, f"Email exception: {e}"


def process_one(req):
    req_id = req["id"]
    report_id = req.get("report_id")
    broadcast_file_id = req.get("notes") if (req.get("action_type") == "broadcast_send" and req.get("notes")) else None
    recipient_id = req.get("recipient_id")
    channel = (req.get("channel") or "").lower()
    action_type = req.get("action_type") or "single_send"
    triggered_by = req.get("requested_by") or "System"

    if not recipient_id or channel not in ("whatsapp", "email"):
        mark_request(req_id, "failed", "Invalid dispatch request payload")
        return False

    try:
        recipient = get_recipient(recipient_id)

        if action_type == "broadcast_send":
            if not broadcast_file_id:
                raise ValueError("Missing broadcast file id in notes")
            bf = get_broadcast_file(broadcast_file_id)
            payload_data = build_payload_from_broadcast(bf)
            linked_report_id = None
        else:
            if not report_id:
                raise ValueError("Missing report id")
            report = get_report(report_id)
            ensure_report_approved(report)
            payload_data = build_payload_from_report(report)
            linked_report_id = report_id

        if channel == "whatsapp":
            ok, msg = send_whatsapp(payload_data, recipient, action_type, triggered_by, linked_report_id)
        else:
            ok, msg = send_email(payload_data, recipient, action_type, triggered_by, linked_report_id)

        mark_request(req_id, "processed" if ok else "failed", msg)
        return ok

    except Exception as e:
        mark_request(req_id, "failed", str(e))
        return False


def main():
    queued = load_queued_requests()
    print(f"[INFO] Loaded {len(queued)} queued dispatch requests")

    success = 0
    failed = 0

    for req in queued:
        ok = process_one(req)
        if ok:
            success += 1
        else:
            failed += 1
        time.sleep(1)

    print(f"[INFO] Queue processing finished. Success={success}, Failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

    

import os
import re
import json
import time
import base64
import requests
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = os.environ.get("FROM_EMAIL", "reports@mail.wiekan.com")
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
        "notes": notes[:2000] if notes else None
    }
    sb.table("dispatch_requests").update(payload).eq("id", request_id).execute()


def insert_dispatch_log(report_id, recipient, channel, destination, action_type, triggered_by):
    resp = (
        sb.table("dispatch_logs")
        .insert({
            "report_id": report_id,
            "recipient_id": recipient.get("id"),
            "recipient_name": recipient.get("name"),
            "channel": channel,
            "destination": destination,
            "action_type": action_type,
            "status": "pending",
            "triggered_by": triggered_by
        })
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
        "sent_at": "now()" if status == "sent" else None
    }
    sb.table("dispatch_logs").update(payload).eq("id", log_id).execute()


def ensure_report_approved(report):
    if (report.get("status") or "").lower() != "approved":
        raise ValueError("Report is not approved. Only approved reports can be dispatched.")


def send_whatsapp(report, recipient, action_type, triggered_by):
    number = normalize_number(recipient.get("phone_number", ""))
    if not number:
        raise ValueError("Invalid WhatsApp phone number")

    payload = {
        "to": number,
        "name": recipient.get("name", "Unknown"),
        "report_date": str(report.get("report_date")),
        "pdf_url": report.get("pdf_url"),
        "caption": (
            f"Doha Bank Market Intelligence\n"
            f"{report.get('report_date')}\n\n"
            f"Please find attached the approved market intelligence report."
        ),
    }

    log_id = insert_dispatch_log(
        report["id"],
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


def send_email(report, recipient, action_type, triggered_by):
    email = (recipient.get("email") or "").strip()
    if not valid_email(email):
        raise ValueError("Invalid email address")

    pdf_url = report.get("pdf_url")
    if not pdf_url:
        raise ValueError("Report PDF URL missing")

    pdf_resp = requests.get(pdf_url, timeout=60)
    if pdf_resp.status_code != 200:
        raise ValueError(f"Could not fetch PDF for email attachment: {pdf_resp.status_code}")

    pdf_b64 = base64.b64encode(pdf_resp.content).decode()

    payload = {
        "from": FROM_EMAIL,
        "to": [email],
        "subject": f"Market Intelligence – {report.get('report_date')}",
        "html": f"""
            <p>Dear {recipient.get('name') or 'Client'},</p>
            <p>Please find attached the approved <strong>Doha Bank Market Intelligence Report</strong>
            for <strong>{report.get('report_date')}</strong>.</p>
        """,
        "attachments": [
            {
                "filename": f"Market-Intelligence-{report.get('report_date')}.pdf",
                "content": pdf_b64,
                "content_type": "application/pdf",
            }
        ],
    }

    log_id = insert_dispatch_log(
        report["id"],
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
            return True, f"Email sent: {res.text}"
        update_dispatch_log(log_id, "failed", res.text)
        return False, f"Email failed: {res.status_code} {res.text}"
    except Exception as e:
        update_dispatch_log(log_id, "failed", str(e))
        return False, f"Email exception: {e}"


def process_one(req):
    req_id = req["id"]
    report_id = req.get("report_id")
    recipient_id = req.get("recipient_id")
    channel = (req.get("channel") or "").lower()
    action_type = req.get("action_type") or "single_send"
    triggered_by = req.get("requested_by") or "System"

    if not report_id or not recipient_id or channel not in ("whatsapp", "email"):
        mark_request(req_id, "failed", "Invalid dispatch request payload")
        return False

    try:
        report = get_report(report_id)
        recipient = get_recipient(recipient_id)
        ensure_report_approved(report)

        if channel == "whatsapp":
            ok, msg = send_whatsapp(report, recipient, action_type, triggered_by)
        else:
            ok, msg = send_email(report, recipient, action_type, triggered_by)

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

    # do not fail whole workflow just because one request failed
    # failures are logged in Supabase
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

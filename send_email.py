    import base64
import datetime
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from supabase_client import get_supabase


RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = os.environ.get("FROM_EMAIL", "DB Strategy Team <updates@market-sigma.com>")
PDF_PATH = Path("report.pdf")
HTML_PATH = Path("report.html")
MARKET_DATA_PATH = Path("market_data.json")
SCHEDULE_ID = "main"
QATAR_TZ = ZoneInfo("Asia/Qatar")


def load_market_data() -> dict:
    if not MARKET_DATA_PATH.exists():
        raise SystemExit("[BLOCKED] market_data.json not found. Email not sent.")

    with MARKET_DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not PDF_PATH.exists():
        raise SystemExit("[BLOCKED] report.pdf not found. Email not sent.")

    status = str(data.get("report_status") or "unknown")
    issues = data.get("validation_issues") or []
    print(f"[INFO] Report status: {status}")
    print(f"[INFO] Validation issue count: {len(issues)}")
    print("[INFO] Phase 1 mode: validation status does not block email dispatch.")

    return data


def format_email_date(value: object | None = None) -> str:
    """Return date in the required email format, for example: 16 May 2026."""
    if isinstance(value, datetime.datetime):
        dt = value.astimezone(QATAR_TZ).date() if value.tzinfo else value.date()
    elif isinstance(value, datetime.date):
        dt = value
    else:
        raw = str(value or "").strip()
        dt = None

        if raw:
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d %B %Y", "%d %b %Y"):
                try:
                    dt = datetime.datetime.strptime(raw[:10] if fmt in ("%Y-%m-%d", "%Y/%m/%d") else raw, fmt).date()
                    break
                except ValueError:
                    continue

            if dt is None:
                try:
                    dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
                except ValueError:
                    dt = None

        if dt is None:
            dt = datetime.datetime.now(QATAR_TZ).date()

    return f"{dt.day} {dt.strftime('%B')} {dt.year}"


def report_date_from_data(data: dict) -> str:
    configured_date = (data.get("config") or {}).get("report_date")
    return format_email_date(configured_date)


def load_email_recipients() -> list[str]:
    sb = get_supabase()
    resp = (
        sb.table("recipients")
        .select("email")
        .eq("channel", "email")
        .eq("active", True)
        .execute()
    )
    rows = resp.data or []

    recipients: list[str] = []
    seen: set[str] = set()
    for row in rows:
        email = str(row.get("email") or "").strip()
        if not email:
            continue
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        recipients.append(email)

    return recipients


def mark_schedule_sent(status: str, message: str) -> None:
    """Best-effort update. Email dispatch must not fail just because this audit marker fails."""
    try:
        sb = get_supabase()
        now_qatar = datetime.datetime.now(QATAR_TZ)
        payload = {
            "last_sent_date": now_qatar.date().isoformat(),
            "last_sent_at": now_qatar.isoformat(),
            "last_send_status": status[:50],
            "last_send_message": message[:500],
        }
        sb.table("report_email_schedule").update(payload).eq("id", SCHEDULE_ID).execute()
        print("✓ Schedule last_sent marker updated in Supabase")
    except Exception as exc:
        print(f"[WARN] Could not update schedule last_sent marker: {exc}")


def build_email_html(report_date: str, has_html: bool = False) -> str:
    if has_html:
        attachment_paragraph = (
            "<p>Please find attached today's market updates, including the latest "
            "market snapshot and key news highlights. The report is provided in two "
            "formats — the <strong>PDF</strong> is best for desktop or printing, and "
            "the <strong>HTML</strong> version is optimised for reading on mobile "
            "devices (open it in any browser).</p>"
        )
    else:
        attachment_paragraph = (
            "<p>Please find attached today's market updates, including the latest "
            "market snapshot and key news highlights.</p>"
        )

    return f"""
        <p>Dear All,</p>

        {attachment_paragraph}

        <p>This report is AI-generated and reflects a snapshot of the previous day's closing rates.</p>

        <p>Kind Regards,</p>

        <p>Strategy Team | AI-generated Daily Updates</p>
    """


def send() -> None:
    data = load_market_data()
    report_date = report_date_from_data(data)

    recipients = load_email_recipients()
    if not recipients:
        print("[WARN] No active email recipients found in Supabase. Email not sent.")
        mark_schedule_sent("no_recipients", "No active email recipients found in Supabase")
        return

    # --- Primary attachment: PDF (always present — load_market_data guards) ---
    with PDF_PATH.open("rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode()

    attachments = [
        {
            "filename": f"Doha-Bank-Market-updates-{report_date}.pdf",
            "content": pdf_b64,
            "content_type": "application/pdf",
        }
    ]

    # --- Optional secondary attachment: HTML companion ---
    # Generated by html_generator.py earlier in the workflow. If the file
    # exists, attach it; if not (e.g. the workflow step was skipped or
    # failed), proceed PDF-only with a log line so it's visible in CI.
    has_html = HTML_PATH.exists()
    if has_html:
        with HTML_PATH.open("rb") as f:
            html_b64 = base64.b64encode(f.read()).decode()
        attachments.append({
            "filename": f"Doha-Bank-Market-updates-{report_date}.html",
            "content": html_b64,
            "content_type": "text/html; charset=utf-8",
        })
        print(f"[INFO] HTML companion attached: {HTML_PATH}")
    else:
        print(f"[INFO] No HTML companion at {HTML_PATH}; sending PDF-only.")

    payload = {
        "from": FROM_EMAIL,
        "to": recipients,
        "subject": f"Doha Bank Market updates - {report_date}",
        "html": build_email_html(report_date, has_html=has_html),
        "attachments": attachments,
    }

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )

    if resp.status_code in (200, 201):
        attach_count = len(attachments)
        suffix = "" if attach_count == 1 else "s"
        print(f"✓ Email sent to {recipients} ({attach_count} attachment{suffix})")
        mark_schedule_sent(
            "sent",
            f"Email sent to {len(recipients)} recipient(s) with {attach_count} attachment{suffix}",
        )
        return

    print(f"[ERROR] Resend API: {resp.status_code} - {resp.text}")
    raise SystemExit(1)


if __name__ == "__main__":
    send()

    

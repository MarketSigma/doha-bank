import base64
import datetime
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from supabase_client import get_supabase


RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = os.environ.get("FROM_EMAIL", "Market Intelligence <updates@market-sigma.com>")
PDF_PATH = Path("report.pdf")
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


def report_date_from_data(data: dict) -> str:
    configured_date = (data.get("config") or {}).get("report_date")
    if configured_date:
        return str(configured_date)
    return datetime.datetime.now(QATAR_TZ).strftime("%d %B %Y")


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


def build_email_html(report_date: str) -> str:
    return f"""
        <p>Dear Team,</p>
        <p>Please find attached the <strong>Doha Bank Market Intelligence Report</strong>
        for <strong>{report_date}</strong>.</p>
        <p>This report was generated automatically from the approved market data sources and distributed according to the saved email schedule.</p>
        <p>Regards,<br/>Market Intelligence Automation</p>
    """


def send() -> None:
    data = load_market_data()
    report_date = report_date_from_data(data)

    recipients = load_email_recipients()
    if not recipients:
        print("[WARN] No active email recipients found in Supabase. Email not sent.")
        mark_schedule_sent("no_recipients", "No active email recipients found in Supabase")
        return

    with PDF_PATH.open("rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode()

    payload = {
        "from": FROM_EMAIL,
        "to": recipients,
        "subject": f"Market Intelligence – {report_date}",
        "html": build_email_html(report_date),
        "attachments": [
            {
                "filename": f"Market-Intelligence-{report_date}.pdf",
                "content": pdf_b64,
                "content_type": "application/pdf",
            }
        ],
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
        print(f"✓ Email sent to {recipients}")
        mark_schedule_sent("sent", f"Email sent to {len(recipients)} recipient(s)")
        return

    print(f"[ERROR] Resend API: {resp.status_code} – {resp.text}")
    raise SystemExit(1)


if __name__ == "__main__":
    send()

import os
import base64
import datetime
import requests
from supabase_client import get_supabase

RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = "reports@mail.wiekan.com"
REPORT_DATE = datetime.date.today().strftime("%d %B %Y")
PDF_PATH = "report.pdf"

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
    return [r["email"] for r in rows if r.get("email")]

def send():
    recipients = load_email_recipients()

    if not recipients:
        print("[WARN] No active email recipients found in Supabase")
        return

    with open(PDF_PATH, "rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode()

    payload = {
        "from": FROM_EMAIL,
        "to": recipients,
        "subject": f"Market Intelligence – {REPORT_DATE}",
        "html": f"""
            <p>Dear Team,</p>
            <p>Please find attached the <strong>Doha Bank Market Intelligence Report</strong>
            for <strong>{REPORT_DATE}</strong>.</p>
            <p>This report is auto-generated daily.</p>
        """,
        "attachments": [
            {
                "filename": f"Market-Intelligence-{REPORT_DATE}.pdf",
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
    else:
        print(f"[ERROR] Resend API: {resp.status_code} – {resp.text}")
        raise SystemExit(1)

if __name__ == "__main__":
    send()

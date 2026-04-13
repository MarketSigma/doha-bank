"""
send_email.py – Sends the PDF report via Resend (free tier: 100 emails/day)
Reads PDF from report.pdf, recipients from RECIPIENTS env var (comma-separated)
"""

import os
import json
import base64
import datetime
import requests

RESEND_API_KEY   = os.environ["RESEND_API_KEY"]
FROM_EMAIL       = "reports@mail.wiekan.com"
REPORT_DATE      = datetime.date.today().strftime("%d %B %Y")
PDF_PATH         = "report.pdf"
RECIPIENTS_FILE  = "recipients.json"


def load_email_recipients() -> list[str]:
    with open(RECIPIENTS_FILE) as f:
        data = json.load(f)
    return [
        r["address"] for r in data.get("email", [])
        if r.get("active", True) and r.get("address")
    ]


RECIPIENTS = load_email_recipients()


def send():
    if not RECIPIENTS:
        print("[WARN] No recipients configured. Set RECIPIENTS secret.")
        return

    with open(PDF_PATH, "rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode()

    payload = {
        "from":    FROM_EMAIL,
        "to":      RECIPIENTS,
        "subject": f"Market Intelligence – {REPORT_DATE}",
        "html": f"""
            <p>Dear Team,</p>
            <p>Please find attached the <strong>Doha Bank Market Intelligence Report</strong>
            for <strong>{REPORT_DATE}</strong>.</p>
            <p>The report covers:</p>
            <ul>
              <li>Global &amp; GCC market indices</li>
              <li>Spot currencies &amp; QAR cross rates</li>
              <li>Commodities &amp; fixed income</li>
              <li>Qatari bank stock performance</li>
              <li>Regional &amp; global news (Reuters, Bloomberg)</li>
              <li>Qatar news (The Peninsula, Qatar Tribune)</li>
            </ul>
            <p>This report is auto-generated daily at 07:00 AST.</p>
        """,
        "attachments": [
            {
                "filename":    f"Market-Intelligence-{REPORT_DATE}.pdf",
                "content":     pdf_b64,
                "content_type": "application/pdf",
            }
        ],
    }

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type":  "application/json",
        },
        json=payload,
        timeout=30,
    )

    if resp.status_code in (200, 201):
        print(f"✓ Email sent to {RECIPIENTS}")
    else:
        print(f"[ERROR] Resend API: {resp.status_code} – {resp.text}")
        raise SystemExit(1)


if __name__ == "__main__":
    send()

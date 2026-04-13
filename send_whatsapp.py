"""
send_whatsapp.py – Sends PDF to all active HNWI WhatsApp numbers
Recipients are managed in recipients.json — no code changes needed to add/remove numbers.

Make.com webhook receives:
  { "to": "97455512345", "filename": "...", "pdf_base64": "...", "caption": "..." }
One webhook call per recipient (Make.com free tier: 1000 ops/month).
"""

import os
import json
import base64
import datetime
import requests
import time

MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
REPORT_DATE      = datetime.date.today().strftime("%d %B %Y")
PDF_PATH         = "report.pdf"
RECIPIENTS_FILE  = "recipients.json"


def load_active_numbers() -> list[dict]:
    with open(RECIPIENTS_FILE) as f:
        data = json.load(f)
    return [r for r in data.get("whatsapp", []) if r.get("active", True)]


def send():
    recipients = load_active_numbers()

    if not recipients:
        print("[WARN] No active WhatsApp recipients found in recipients.json")
        return

    with open(PDF_PATH, "rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode()

    filename = f"Market-Intelligence-{REPORT_DATE}.pdf"
    caption  = (
        f"📊 *Doha Bank Market Intelligence*\n"
        f"_{REPORT_DATE}_\n\n"
        f"Daily report covering global indices, GCC markets, "
        f"currencies, commodities and latest news.\n\n"
        f"_This report is generated exclusively for Doha Bank HNWI clients._"
    )

    success_count = 0
    fail_count    = 0

    for recipient in recipients:
        number = recipient.get("number", "").strip()
        name   = recipient.get("name", "Unknown")

        if not number:
            print(f"[WARN] Skipping {name} — no number defined")
            continue

        payload = {
            "to":          number,
            "name":        name,
            "report_date": REPORT_DATE,
            "filename":    filename,
            "pdf_base64":  pdf_b64,
            "caption":     caption,
        }

        try:
            resp = requests.post(
                MAKE_WEBHOOK_URL,
                json=payload,
                timeout=60,
            )
            if resp.status_code == 200:
                print(f"  ✓ Sent to {name} ({number})")
                success_count += 1
            else:
                print(f"  [ERROR] {name} ({number}): {resp.status_code} – {resp.text}")
                fail_count += 1

        except Exception as e:
            print(f"  [ERROR] {name} ({number}): {e}")
            fail_count += 1

        # Small delay between sends to avoid rate limiting
        time.sleep(1.5)

    print(f"\n✓ WhatsApp delivery complete: {success_count} sent, {fail_count} failed")

    if fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    send()

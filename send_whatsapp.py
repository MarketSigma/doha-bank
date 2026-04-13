import os
import datetime
import requests
import time
from supabase_client import get_supabase

MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
REPORT_DATE = datetime.date.today().strftime("%d %B %Y")
GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "wiekan-ou")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "doha-bank-mi")
PDF_URL = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/main/report.pdf"


def load_active_numbers() -> list[dict]:
    sb = get_supabase()

    resp = (
        sb.table("recipients")
        .select("name, phone_number, tier")
        .eq("channel", "whatsapp")
        .eq("active", True)
        .execute()
    )

    return resp.data or []


def send():
    recipients = load_active_numbers()

    if not recipients:
        print("[WARN] No active WhatsApp recipients found in Supabase")
        return

    caption = (
        f"Doha Bank Market Intelligence\n"
        f"{REPORT_DATE}\n\n"
        f"Please find attached today's market intelligence report covering "
        f"global indices, GCC markets, currencies, commodities, and latest news."
    )

    success_count = 0
    fail_count = 0

    for recipient in recipients:
        number = (recipient.get("phone_number") or "").strip()
        name = recipient.get("name", "Unknown")

        if number and not number.startswith("+"):
            number = f"+{number}"

        if not number:
            print(f"[WARN] Skipping {name}, no phone number defined")
            continue

        payload = {
            "to": number,
            "name": name,
            "report_date": REPORT_DATE,
            "pdf_url": PDF_URL,
            "caption": caption,
        }

        try:
            resp = requests.post(
                MAKE_WEBHOOK_URL,
                json=payload,
                timeout=60,
            )

            if resp.status_code == 200:
                print(f"✓ Sent to {name} ({number})")
                success_count += 1
            else:
                print(f"[ERROR] {name} ({number}): {resp.status_code} | {resp.text}")
                fail_count += 1

        except Exception as e:
            print(f"[ERROR] {name} ({number}): {e}")
            fail_count += 1

        time.sleep(1.5)

    print(f"WhatsApp delivery complete: {success_count} sent, {fail_count} failed")

    if fail_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    send()

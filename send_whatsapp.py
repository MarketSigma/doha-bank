import json
import os
import datetime
import requests
import time
import re
from supabase_client import get_supabase

MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
REPORT_DATE = datetime.date.today().strftime("%d %B %Y")
SIGNED_URL_FILE = "signed_pdf_url.json"


def load_active_numbers() -> list[dict]:
    sb = get_supabase()

    resp = (
        sb.table("recipients")
        .select("id, name, phone_number, tier")
        .eq("channel", "whatsapp")
        .eq("active", True)
        .execute()
    )

    rows = resp.data or []
    print(f"[INFO] Loaded {len(rows)} active WhatsApp recipients from Supabase")
    return rows


def normalize_number(number: str) -> str:
    """
    Require strict international format only.
    Example:
      +97455512345
      +96170123456
      +447700900123
    """
    if not number:
        return ""

    number = number.strip().replace(" ", "")

    if not number.startswith("+"):
        return ""

    if not re.fullmatch(r"\+\d{8,15}", number):
        return ""

    return number


def load_signed_pdf_url() -> str:
    if not os.path.exists(SIGNED_URL_FILE):
        print(f"[ERROR] Signed URL file not found: {SIGNED_URL_FILE}")
        raise SystemExit(1)

    with open(SIGNED_URL_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    url = data.get("signed_url", "").strip()
    if not url:
        print("[ERROR] signed_url missing in signed_pdf_url.json")
        raise SystemExit(1)

    return url


def send():
    recipients = load_active_numbers()

    if not recipients:
        print("[WARN] No active WhatsApp recipients found in Supabase")
        return

    pdf_url = load_signed_pdf_url()
    print(f"[INFO] Using signed PDF URL: {pdf_url}")

    caption = (
        f"Doha Bank Market Intelligence\n"
        f"{REPORT_DATE}\n\n"
        f"Please find attached today's market intelligence report covering "
        f"global indices, GCC markets, currencies, commodities, and latest news."
    )

    success_count = 0
    fail_count = 0

    for recipient in recipients:
        recipient_id = recipient.get("id", "")
        name = recipient.get("name", "Unknown")
        raw_number = recipient.get("phone_number", "")
        number = normalize_number(raw_number)

        if not number:
            print(f"[WARN] Skipping {name}, invalid phone format: {raw_number}")
            fail_count += 1
            continue

        payload = {
            "recipient_id": recipient_id,
            "to": number,
            "name": name,
            "report_date": REPORT_DATE,
            "pdf_url": pdf_url,
            "caption": caption,
        }

        print(f"[INFO] Sending WhatsApp to {name} | {number}")
        print(f"[DEBUG] Payload: {payload}")

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

import json
import os
import datetime
import requests
import time
import re
from supabase_client import get_supabase

MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
REPORT_DATE = datetime.date.today().strftime("%d %B %Y")
PUBLIC_URL_FILE = "public_pdf_url.json"


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
    if not number:
        return ""

    number = number.strip().replace(" ", "")

    if not number.startswith("+"):
        return ""

    if not re.fullmatch(r"\+\d{8,15}", number):
        return ""

    return number


def load_public_pdf_url() -> str:
    if not os.path.exists(PUBLIC_URL_FILE):
        print(f"[ERROR] Public URL file not found: {PUBLIC_URL_FILE}")
        raise SystemExit(1)

    with open(PUBLIC_URL_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    url = data.get("public_url", "").strip()
    if not url:
        print("[ERROR] public_url missing in public_pdf_url.json")
        raise SystemExit(1)

    return url


def validate_pdf_url(url: str) -> bool:
    try:
        r = requests.get(url, timeout=20)
        print(f"[INFO] PDF URL validation status: {r.status_code}")
        content_type = r.headers.get("content-type", "")
        print(f"[INFO] PDF URL content-type: {content_type}")
        return r.status_code == 200 and "pdf" in content_type.lower()
    except Exception as e:
        print(f"[ERROR] PDF URL validation failed: {e}")
        return False


def send():
    recipients = load_active_numbers()

    if not recipients:
        print("[WARN] No active WhatsApp recipients found in Supabase")
        return

    pdf_url = load_public_pdf_url()
    print(f"[INFO] Using public PDF URL: {pdf_url}")

    if not validate_pdf_url(pdf_url):
        print("[ERROR] Public PDF URL is not reachable as a PDF")
        raise SystemExit(1)

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

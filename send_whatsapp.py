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


def load_active_numbers():
    sb = get_supabase()

    resp = (
        sb.table("recipients")
        .select("id, name, phone_number, tier")
        .eq("channel", "whatsapp")
        .eq("active", True)
        .execute()
    )

    rows = resp.data or []
    print(f"[INFO] Loaded {len(rows)} active WhatsApp recipients")
    return rows


def normalize_number(number: str):
    if not number:
        return ""

    number = number.strip().replace(" ", "")

    if not number.startswith("+"):
        return ""

    if not re.fullmatch(r"\+\d{8,15}", number):
        return ""

    return number


def load_public_pdf_url():
    if not os.path.exists(PUBLIC_URL_FILE):
        print("[ERROR] public_pdf_url.json NOT FOUND")
        raise SystemExit(1)

    with open(PUBLIC_URL_FILE, "r") as f:
        data = json.load(f)

    print(f"[DEBUG] public_pdf_url.json content: {data}")

    url = data.get("public_url", "")

    if not url or "githubusercontent" in url:
        print("[ERROR] INVALID PDF URL DETECTED")
        raise SystemExit(1)

    return url


def send():
    recipients = load_active_numbers()

    if not recipients:
        print("[WARN] No recipients found")
        return

    pdf_url = load_public_pdf_url()

    print(f"[INFO] FINAL PDF URL USED: {pdf_url}")

    caption = (
        f"Doha Bank Market Intelligence\n"
        f"{REPORT_DATE}\n\n"
        f"Please find attached today's market intelligence report."
    )

    for r in recipients:
        name = r.get("name")
        number = normalize_number(r.get("phone_number"))

        if not number:
            print(f"[WARN] Skipping {name}, invalid number")
            continue

        payload = {
            "to": number,
            "name": name,
            "report_date": REPORT_DATE,
            "pdf_url": pdf_url,
            "caption": caption
        }

        print(f"[INFO] Sending to {number}")
        print(f"[DEBUG] Payload: {payload}")

        res = requests.post(MAKE_WEBHOOK_URL, json=payload)

        print(f"[INFO] Response: {res.status_code} {res.text}")

        time.sleep(1.5)


if __name__ == "__main__":
    send()

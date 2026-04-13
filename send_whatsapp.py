"""
send_whatsapp.py
Triggers Make.com webhook → 2Chat → WhatsApp
Sends the PDF as a public GitHub URL (no base64 needed)
"""
import os, json, requests, time, datetime

MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
GITHUB_OWNER     = os.environ.get("GITHUB_OWNER", "Wiekan-ou")
GITHUB_REPO      = os.environ.get("GITHUB_REPO",  "doha-bank-mi")
REPORT_DATE      = datetime.date.today().strftime("%d %B %Y")
RECIPIENTS_FILE  = "recipients.json"

# Public URL to the PDF stored in the GitHub repo
PDF_URL = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/main/report.pdf"

def load_recipients():
    with open(RECIPIENTS_FILE) as f:
        data = json.load(f)
    return [r for r in data.get("whatsapp", []) if r.get("active", True)]

def send():
    recipients = load_recipients()
    if not recipients:
        print("[WARN] No active WhatsApp recipients.")
        return

    caption = (
        f"*Doha Bank Market Intelligence*\n"
        f"_{REPORT_DATE}_\n\n"
        f"Please find today's Market Intelligence Report attached.\n"
        f"Covering: Global Indices · GCC Markets · Currencies · "
        f"Commodities · Qatar News\n\n"
        f"_Strictly Confidential — Doha Bank HNWI Clients Only_"
    )

    success = 0
    failed  = 0

    for r in recipients:
        number = r.get("number", "").strip()
        name   = r.get("name",   "Unknown")

        if not number:
            print(f"[WARN] Skipping {name} — no number")
            continue

        # Format number with + prefix for 2Chat
        if not number.startswith("+"):
            number = "+" + number

        payload = {
            "to":       number,
            "name":     name,
            "pdf_url":  PDF_URL,
            "caption":  caption,
            "report_date": REPORT_DATE,
        }

        try:
            resp = requests.post(MAKE_WEBHOOK_URL, json=payload, timeout=30)
            if resp.status_code == 200:
                print(f"  ✓ Sent to {name} ({number})")
                success += 1
            else:
                print(f"  [ERROR] {name}: {resp.status_code} — {resp.text}")
                failed += 1
        except Exception as e:
            print(f"  [ERROR] {name}: {e}")
            failed += 1

        time.sleep(2)  # avoid rate limiting

    print(f"\n✓ WhatsApp done: {success} sent, {failed} failed")
    if failed > 0:
        raise SystemExit(1)

if __name__ == "__main__":
    send()

import base64
import datetime
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from supabase_client import get_supabase
from email_body_generator import build_email_body


RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = os.environ.get("FROM_EMAIL", "DB Strategy Team <updates@market-sigma.com>")
# Visible "To:" address. All actual recipients go into BCC so they can't see
# each other. The To: header needs to be a real deliverable address (some
# email providers flag empty / placeholder To: fields as spam), so we point
# it at the sender's own mailbox by default. The display name "DB Strategy
# Team" is what recipients see in their To: column; the address itself only
# shows when they expand the header. Override via env if needed.
TO_HEADER_EMAIL = os.environ.get(
    "TO_HEADER_EMAIL",
    "DB Strategy Team <updates@market-sigma.com>",
)
MARKET_DATA_PATH = Path("market_data.json")

# Doha Bank lockup, embedded inline via CID so recipients never depend on an
# external image host. Resolved against several likely locations (repo root,
# docs/, alongside this script) so it works regardless of the working
# directory the workflow runs from. Override with LOGO_PATH if needed.
LOGO_FILENAME = "doha_bank_logo@2x.png"
LOGO_CID = "doha-logo"


def resolve_logo_path() -> Path | None:
    """Find the logo wherever it lives in the repo. Returns None if absent."""
    override = os.environ.get("LOGO_PATH")
    if override:
        p = Path(override)
        return p if p.exists() else None

    here = Path(__file__).resolve().parent
    candidates = [
        Path(LOGO_FILENAME),                 # cwd
        Path("docs") / LOGO_FILENAME,        # cwd/docs
        here / LOGO_FILENAME,                # next to this script
        here / "docs" / LOGO_FILENAME,       # repo docs/ folder
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


LOGO_PATH = resolve_logo_path()

SCHEDULE_ID = "main"
QATAR_TZ = ZoneInfo("Asia/Qatar")
VALIDATION_ALERT_RECIPIENTS = [
    "smajari@dohabank.com.qa",
    "salam.majari@gmail.com",
]


def load_market_data() -> dict:
    if not MARKET_DATA_PATH.exists():
        raise SystemExit("[BLOCKED] market_data.json not found. Email not sent.")

    with MARKET_DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

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


def build_logo_attachment() -> dict | None:
    """Return a Resend attachment dict for the inline logo, or None if the
    file is missing. Missing logo is not fatal: the email body falls back to
    the wordmark set in type rather than showing a broken image."""
    if LOGO_PATH is None:
        print(f"[WARN] Logo '{LOGO_FILENAME}' not found (looked in cwd, docs/, "
              f"and beside send_email.py). Sending without inline logo.")
        return None

    encoded = base64.b64encode(LOGO_PATH.read_bytes()).decode()
    print(f"[INFO] Inline logo attached: {LOGO_PATH} ({LOGO_PATH.stat().st_size:,} bytes)")
    return {
        "content": encoded,
        "filename": LOGO_PATH.name,
        "content_id": LOGO_CID,
    }


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


def send_validation_alert(data: dict) -> None:
    issues = data.get("validation_issues") or []

    if not issues:
        return

    body = (
        "The Doha Bank Market Intelligence report was sent successfully.\n\n"
        "However, validation issues were detected:\n\n"
        + "\n".join(f"- {i}" for i in issues)
    )

    payload = {
        "from": FROM_EMAIL,
        "to": VALIDATION_ALERT_RECIPIENTS,
        "subject": "Doha Bank Dashboard Validation Alert",
        "text": body,
    }

    requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )


def send() -> None:
    data = load_market_data()
    report_date = report_date_from_data(data)

    recipients = load_email_recipients()
    if not recipients:
        print("[WARN] No active email recipients found in Supabase. Email not sent.")
        mark_schedule_sent("no_recipients", "No active email recipients found in Supabase")
        return

    # Attach the logo inline and point the renderer at its CID. Done before
    # build_email_body so the <img src="cid:..."> lands in the markup.
    logo_attachment = build_logo_attachment()
    config = data.setdefault("config", {})
    config["logo_url"] = f"cid:{LOGO_CID}" if logo_attachment else ""

    report_title = config.get("report_title", "Market Intelligence")

    # Full report is embedded in the email body via email_body_generator.
    # No attachments other than the inline logo — recipients read the report
    # inline. The PDF that the workflow still generates is retained for the
    # dashboard / archive only; it does not go out via email anymore.
    body_html = build_email_body(data)
    print(f"[INFO] Email body rendered: {len(body_html):,} characters")

    # Privacy: every recipient goes into BCC so they cannot see who else
    # received the report. Resend requires a non-empty To: field; we send
    # it to the sender's own configured mailbox. Each recipient sees only
    # their own address in the headers — the rest are blind-copied.
    payload = {
        "from": FROM_EMAIL,
        "to": [TO_HEADER_EMAIL],
        "bcc": recipients,
        "subject": f"Doha Bank {report_title} - {report_date}",
        "html": body_html,
    }
    if logo_attachment:
        payload["attachments"] = [logo_attachment]

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
        print(f"✓ Email sent — To:{TO_HEADER_EMAIL}, BCC:{len(recipients)} recipient(s)")
        mark_schedule_sent(
            "sent",
            f"Email sent — {len(recipients)} BCC recipient(s) — body-only",
        )
        # Case-insensitive: the pipeline writes "NEEDS_REVIEW" in upper case.
        if str(data.get("report_status") or "").strip().lower() == "needs_review":
            send_validation_alert(data)
        return

    print(f"[ERROR] Resend API: {resp.status_code} - {resp.text}")
    raise SystemExit(1)


if __name__ == "__main__":
    send()

    

import datetime as dt
import os
import sys
from zoneinfo import ZoneInfo

import requests


SCHEDULE_TABLE = "report_email_schedule"
SCHEDULE_ID = "main"
DEFAULT_TIMEZONE = "Asia/Qatar"
DEFAULT_WINDOW_MINUTES = 5


def github_output(**values: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        for key, value in values.items():
            print(f"{key}={value}")
        return

    with open(output_path, "a", encoding="utf-8") as f:
        for key, value in values.items():
            safe_value = "" if value is None else str(value)
            f.write(f"{key}={safe_value}\n")


def fail_closed(reason: str) -> None:
    print(f"[SKIP] {reason}")
    github_output(
        run_report="false",
        reason=reason,
        saved_time="",
        current_qatar_time="",
    )
    raise SystemExit(0)


def get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def supabase_get_schedule() -> dict:
    supabase_url = get_required_env("SUPABASE_URL").rstrip("/")
    supabase_key = get_required_env("SUPABASE_SERVICE_ROLE_KEY")

    url = f"{supabase_url}/rest/v1/{SCHEDULE_TABLE}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }
    params = {
        "id": f"eq.{SCHEDULE_ID}",
        "select": "*",
        "limit": "1",
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    rows = response.json() or []
    if not rows:
        raise RuntimeError(f"No row found in {SCHEDULE_TABLE} where id = {SCHEDULE_ID}")
    return rows[0]


def parse_send_time(value: str) -> dt.time:
    if not value:
        raise ValueError("send_time is empty")
    return dt.time.fromisoformat(str(value)[:8])


def parse_last_sent_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def is_truthy(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")

    if event_name == "workflow_dispatch":
        now_qatar = dt.datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
        print("[RUN] Manual workflow_dispatch detected. Bypassing schedule check.")
        github_output(
            run_report="true",
            reason="manual_dispatch",
            saved_time="manual",
            current_qatar_time=now_qatar.strftime("%Y-%m-%d %H:%M:%S %Z"),
        )
        return

    try:
        schedule = supabase_get_schedule()
    except Exception as exc:
        fail_closed(f"Could not read Supabase email schedule: {exc}")
        return

    active = bool(schedule.get("active"))
    if not active:
        fail_closed("Email schedule is inactive")
        return

    timezone_name = schedule.get("timezone") or DEFAULT_TIMEZONE
    try:
        tz = ZoneInfo(timezone_name)
    except Exception:
        print(f"[WARN] Invalid timezone {timezone_name!r}. Falling back to {DEFAULT_TIMEZONE}.")
        timezone_name = DEFAULT_TIMEZONE
        tz = ZoneInfo(DEFAULT_TIMEZONE)

    now = dt.datetime.now(tz)

    run_weekdays_only = is_truthy(os.environ.get("RUN_WEEKDAYS_ONLY"), default=True)
    if run_weekdays_only and now.weekday() >= 5:
        github_output(
            run_report="false",
            reason="weekend_skip",
            saved_time=str(schedule.get("send_time", "")),
            current_qatar_time=now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        )
        print(f"[SKIP] Weekend in {timezone_name}. No report dispatch.")
        return

    send_time = parse_send_time(schedule.get("send_time"))
    scheduled_dt = dt.datetime.combine(now.date(), send_time, tzinfo=tz)

    try:
        window_minutes = int(os.environ.get("SCHEDULE_WINDOW_MINUTES", DEFAULT_WINDOW_MINUTES))
    except Exception:
        window_minutes = DEFAULT_WINDOW_MINUTES
    window_minutes = max(1, min(window_minutes, 30))

    window_end = scheduled_dt + dt.timedelta(minutes=window_minutes)
    in_window = scheduled_dt <= now < window_end

    last_sent_date = parse_last_sent_date(schedule.get("last_sent_date"))
    if last_sent_date == now.date():
        github_output(
            run_report="false",
            reason="already_sent_today",
            saved_time=str(schedule.get("send_time", "")),
            current_qatar_time=now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        )
        print(f"[SKIP] Report already sent today: {last_sent_date.isoformat()}")
        return

    if in_window:
        github_output(
            run_report="true",
            reason="schedule_due",
            saved_time=str(schedule.get("send_time", "")),
            current_qatar_time=now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        )
        print(
            "[RUN] Schedule is due. "
            f"Now={now.strftime('%Y-%m-%d %H:%M:%S %Z')} "
            f"Saved={schedule.get('send_time')} "
            f"Window={window_minutes}m"
        )
        return

    github_output(
        run_report="false",
        reason="not_due",
        saved_time=str(schedule.get("send_time", "")),
        current_qatar_time=now.strftime("%Y-%m-%d %H:%M:%S %Z"),
    )
    print(
        "[SKIP] Schedule is not due. "
        f"Now={now.strftime('%Y-%m-%d %H:%M:%S %Z')} "
        f"Saved={schedule.get('send_time')} "
        f"DueWindow={scheduled_dt.strftime('%H:%M')} to {window_end.strftime('%H:%M')}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] Schedule check failed closed: {exc}", file=sys.stderr)
        github_output(
            run_report="false",
            reason=f"schedule_check_error: {exc}",
            saved_time="",
            current_qatar_time="",
        )
        raise SystemExit(0)

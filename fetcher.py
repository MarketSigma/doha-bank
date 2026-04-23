import json
import os
import re
import datetime
from html import unescape
from email.utils import parsedate_to_datetime
from typing import Optional, List
from urllib.parse import quote

import feedparser
import yfinance as yf
import anthropic
import requests
import pandas as pd


CONFIG = {
    "client_name": "Doha Bank",
    "report_date": datetime.date.today().strftime("%d %B %Y"),
    "delivery_time_ast": "07:00",
    "sections": {
        "global_indices": True,
        "gcc_indices": True,
        "spot_currency": True,
        "qar_cross_rates": True,
        "fixed_income": True,
        "qatari_banks": True,
        "commodities": True,
        "global_news": True,
        "qatar_news": True,
    }
}

GLOBAL_INDICES = {
    "US S&P 500": "^GSPC",
    "UK FTSE 100": "^FTSE",
    "Japan Nikkei": "^N225",
    "Germany DAX": "^GDAXI",
    "Hong Kong HSI": "^HSI",
    "India Sensex": "^BSESN",
}

# GCC is now sourced from Supabase gcc_indices_history, not Yahoo directly.
GCC_INDEX_CONFIG = [
    {"code": "QE", "name": "Qatar QE Index"},
    {"code": "TASI", "name": "Saudi Tadawul"},
    {"code": "DFMGI", "name": "Dubai DFM"},
    {"code": "FADGI", "name": "Abu Dhabi ADX"},
    {"code": "BKA", "name": "Kuwait Boursa"},
    {"code": "BHBX", "name": "Bahrain"},
]

SPOT_CURRENCY = {
    "USD Index": "DX-Y.NYB",
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "JPY=X",
    "USD/CNY": "CNY=X",
}

QATARI_BANKS = {
    "Doha": "DHBK.QA",
    "QNB": "QNBK.QA",
    "QIB": "QIBK.QA",
    "CBQ": "CBQK.QA",
    "QIIB": "QIIK.QA",
    "Al Rayan": "MARK.QA",
    "Dukhan": "DUBK.QA",
    "Ahli": "ABQK.QA",
}

COMMODITIES = {
    "Brent Crude": "BZ=F",
    "Gold (USD)": "GC=F",
    "Silver": "SI=F",
}

FIXED_INCOME = {
    "UST 5-Year": "^FVX",
    "UST 10-Year": "^TNX",
}

NEWS_FEEDS = {
    "global": [
        {
            "source": "Reuters",
            "url": "https://feeds.reuters.com/reuters/businessNews",
            "max": 10,
        },
        {
            "source": "Bloomberg",
            "url": "https://feeds.bloomberg.com/markets/news.rss",
            "max": 10,
        },
    ],
    "qatar": [
        {
            "source": "The Peninsula",
            "url": "https://thepeninsulaqatar.com/rss/business",
            "max": 10,
        },
        {
            "source": "Qatar Tribune",
            "url": "https://www.qatar-tribune.com/rss",
            "max": 10,
        },
    ],
}

QATAR_NEWS_TARGET_COUNT = 4
QATAR_NEWS_MAX_AGE_HOURS = 24
QATAR_NEWS_REQUIRED_KEYWORDS = [
    "business", "economy", "economic", "investment", "investor", "investors",
    "bank", "banking", "finance", "financial", "market", "markets", "stocks",
    "trade", "trading", "company", "companies", "ipo", "bond", "bonds",
    "sukuk", "merger", "acquisition", "real estate", "energy", "gas", "lng"
]
QATAR_NEWS_EXCLUDE_KEYWORDS = [
    "world cup", "sports", "football", "fifa", "match", "cup", "covid-19", "covid"
]
QATAR_NEWS_FALLBACK_QUERIES = [
    {
        "source": "Google News / Qatar business approved",
        "query": 'Qatar (business OR economy OR investment OR bank OR banking OR finance OR market) when:1d (site:thepeninsulaqatar.com OR site:qatar-tribune.com)',
        "max": 12,
    },
    {
        "source": "Google News / Qatar business reputable",
        "query": 'Qatar (business OR economy OR investment OR bank OR banking OR finance OR market) when:1d (site:thepeninsulaqatar.com OR site:qatar-tribune.com OR site:gulf-times.com OR site:reuters.com OR site:bloomberg.com)',
        "max": 12,
    },
    {
        "source": "Google News / Qatar investment broad",
        "query": 'Qatar (business OR economy OR investment OR bank OR banking OR finance OR market) when:1d',
        "max": 12,
    },
]


def _to_float(val) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None


def _fmt_pct_number(current: Optional[float], base: Optional[float], digits: int = 1) -> str:
    if current is None or base in (None, 0):
        return "N/A"
    pct = ((current - base) / base) * 100
    return f"{pct:+.{digits}f}%"


def _fmt_pct_value(val: Optional[float], digits: int = 1) -> str:
    if val is None:
        return "N/A"
    return f"{val:+.{digits}f}%"


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_close_series(df):
    if df is None or df.empty:
        return None

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = df.columns.get_level_values(0)

    col = "Adj Close" if "Adj Close" in df.columns else "Close"
    if col not in df.columns:
        return None

    closes = df[col].dropna()
    if closes.empty:
        return None

    return closes.sort_index()


def _safe_download(sym: str, start: datetime.date):
    try:
        df = yf.download(
            tickers=sym,
            start=start.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=False,
            repair=True,
            progress=False,
            threads=False,
        )
        return _extract_close_series(df)
    except Exception as e:
        print(f"[WARN] download failed for {sym}: {e}")
        return None


def _safe_ticker_history(sym: str, period: str = "1y"):
    try:
        ticker = yf.Ticker(sym)
        df = ticker.history(
            period=period,
            interval="1d",
            auto_adjust=False,
            actions=False,
        )
        return _extract_close_series(df)
    except Exception as e:
        print(f"[WARN] ticker.history failed for {sym}: {e}")
        return None


def _safe_yahoo_chart_api(sym: str, range_str: str = "1y", interval: str = "1d"):
    try:
        encoded_sym = quote(sym, safe="")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_sym}"
        params = {
            "range": range_str,
            "interval": interval,
            "includePrePost": "false",
            "events": "div,splits",
        }
        headers = {"User-Agent": "Mozilla/5.0"}

        r = requests.get(url, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        payload = r.json()

        result = payload.get("chart", {}).get("result", [])
        error_obj = payload.get("chart", {}).get("error")

        if error_obj:
            print(f"[WARN] Yahoo chart API returned error for {sym}: {error_obj}")
            return None

        if not result:
            print(f"[WARN] Yahoo chart API returned no result for {sym}")
            return None

        result0 = result[0]
        timestamps = result0.get("timestamp", [])
        indicators = result0.get("indicators", {})

        adjclose = indicators.get("adjclose", [])
        quote_block = indicators.get("quote", [])

        closes = None
        if adjclose and isinstance(adjclose, list):
            closes = adjclose[0].get("adjclose")
        if not closes and quote_block and isinstance(quote_block, list):
            closes = quote_block[0].get("close")

        if not timestamps or not closes:
            print(f"[WARN] Yahoo chart API missing timestamps/closes for {sym}")
            return None

        pairs = []
        for ts, px in zip(timestamps, closes):
            if px is None:
                continue
            dt = datetime.datetime.utcfromtimestamp(ts)
            pairs.append((dt, float(px)))

        if not pairs:
            print(f"[WARN] Yahoo chart API no usable points for {sym}")
            return None

        s = pd.Series(
            data=[p[1] for p in pairs],
            index=[p[0] for p in pairs],
            dtype="float64"
        ).sort_index()

        return s if len(s) >= 2 else None

    except Exception as e:
        print(f"[WARN] Yahoo chart API failed for {sym}: {e}")
        return None


def _get_series(sym: str, start: datetime.date):
    closes = _safe_download(sym, start)
    if closes is not None and len(closes) >= 2:
        return closes

    print(f"[INFO] Falling back to ticker.history for {sym}")
    closes = _safe_ticker_history(sym, period="1y")
    if closes is not None and len(closes) >= 2:
        return closes

    print(f"[INFO] Falling back to Yahoo chart API for {sym}")
    closes = _safe_yahoo_chart_api(sym, range_str="1y", interval="1d")
    if closes is not None and len(closes) >= 2:
        return closes

    return None


def _last_value_before_or_on(closes, target_date: datetime.date) -> Optional[float]:
    eligible = []
    for dt_idx, px in closes.items():
        try:
            d = dt_idx.date() if hasattr(dt_idx, "date") else datetime.datetime.strptime(str(dt_idx)[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if d <= target_date:
            eligible.append(_to_float(px))
    eligible = [x for x in eligible if x is not None]
    return eligible[-1] if eligible else None


def _fetch_market_row(name: str, sym: str, today: datetime.date, digits: int = 2) -> dict:
    year_start = datetime.date(today.year, 1, 1)
    month_start = datetime.date(today.year, today.month, 1)
    history_start = year_start - datetime.timedelta(days=20)

    closes = _get_series(sym, history_start)
    if closes is None or len(closes) < 2:
        return {
            "name": name,
            "ticker": sym,
            "px_last": "N/A",
            "change_1d": "N/A",
            "mtd": "N/A",
            "ytd": "N/A",
            "as_of": None,
            "source": "Yahoo Finance",
        }

    px_last = _to_float(closes.iloc[-1])
    px_prev = _to_float(closes.iloc[-2]) if len(closes) >= 2 else None

    month_base = _last_value_before_or_on(closes, month_start - datetime.timedelta(days=1))
    year_base = _last_value_before_or_on(closes, year_start - datetime.timedelta(days=1))

    as_of = closes.index[-1]
    as_of_str = as_of.strftime("%Y-%m-%d") if hasattr(as_of, "strftime") else str(as_of)[:10]

    return {
        "name": name,
        "ticker": sym,
        "px_last": round(px_last, digits) if px_last is not None else "N/A",
        "change_1d": _fmt_pct_number(px_last, px_prev, 1),
        "mtd": _fmt_pct_number(px_last, month_base, 1),
        "ytd": _fmt_pct_number(px_last, year_base, 1),
        "as_of": as_of_str,
        "source": "Yahoo Finance",
    }


# ------------------------------
# GCC indices from Supabase
# ------------------------------

def _supabase_headers() -> dict:
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    return {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }


def _supabase_base_url() -> Optional[str]:
    return os.environ.get("SUPABASE_URL")


def _safe_gcc_history_from_supabase(instrument_code: str, today: datetime.date) -> list[dict]:
    supabase_url = _supabase_base_url()
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        print("[WARN][SUPABASE_GCC] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
        return []

    today_str = today.strftime("%Y-%m-%d")
    url = (
        f"{supabase_url}/rest/v1/gcc_indices_history"
        f"?select=*"
        f"&instrument_code=eq.{quote(instrument_code, safe='')}"
        f"&status=eq.ok"
        f"&as_of_date=lte.{today_str}"
        f"&order=as_of_date.asc,created_at.asc"
    )

    try:
        r = requests.get(url, headers=_supabase_headers(), timeout=20)
        r.raise_for_status()
        rows = r.json()
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"[WARN][SUPABASE_GCC] Failed reading {instrument_code}: {e}")
        return []


def _dedupe_history_rows(rows: list[dict]) -> list[dict]:
    # Keep the latest created row per as_of_date.
    by_date: dict[str, dict] = {}
    for row in rows:
        d = row.get("as_of_date")
        if not d or not row.get("instrument_code"):
            continue
        existing = by_date.get(d)
        if existing is None or str(row.get("created_at", "")) > str(existing.get("created_at", "")):
            by_date[d] = row
    return [by_date[d] for d in sorted(by_date.keys())]


def _row_px(row: Optional[dict]) -> Optional[float]:
    if not row:
        return None
    return _to_float(row.get("px_last"))


def _find_latest_row(rows: list[dict]) -> Optional[dict]:
    return rows[-1] if rows else None


def _find_previous_row(rows: list[dict], latest_date: str) -> Optional[dict]:
    prev = [r for r in rows if r.get("as_of_date") and r.get("as_of_date") < latest_date]
    return prev[-1] if prev else None


def _find_month_reference(rows: list[dict], latest_date: datetime.date) -> Optional[dict]:
    month_rows = []
    for r in rows:
        try:
            d = datetime.date.fromisoformat(r["as_of_date"])
        except Exception:
            continue
        if d.year == latest_date.year and d.month == latest_date.month and d <= latest_date:
            month_rows.append(r)
    if month_rows:
        return month_rows[0]

    before_month = []
    month_start = datetime.date(latest_date.year, latest_date.month, 1)
    for r in rows:
        try:
            d = datetime.date.fromisoformat(r["as_of_date"])
        except Exception:
            continue
        if d < month_start:
            before_month.append(r)
    return before_month[-1] if before_month else None


def _find_year_reference(rows: list[dict], latest_date: datetime.date) -> Optional[dict]:
    year_rows = []
    for r in rows:
        try:
            d = datetime.date.fromisoformat(r["as_of_date"])
        except Exception:
            continue
        if d.year == latest_date.year and d <= latest_date:
            year_rows.append(r)
    return year_rows[0] if year_rows else None


def _calc_pct_from_two_values(current: Optional[float], base: Optional[float]) -> Optional[float]:
    if current is None or base in (None, 0):
        return None
    return ((current - base) / base) * 100


def _build_gcc_row(name: str, instrument_code: str, today: datetime.date) -> dict:
    rows = _dedupe_history_rows(_safe_gcc_history_from_supabase(instrument_code, today))
    latest = _find_latest_row(rows)
    if not latest:
        return {
            "name": name,
            "ticker": instrument_code,
            "px_last": "N/A",
            "change_1d": "N/A",
            "mtd": "N/A",
            "ytd": "N/A",
            "as_of": None,
            "source": "Supabase gcc_indices_history",
        }

    latest_date = datetime.date.fromisoformat(latest["as_of_date"])
    latest_px = _row_px(latest)

    prev_row = _find_previous_row(rows, latest["as_of_date"])
    prev_px = _row_px(prev_row)
    if prev_px is None:
        prev_px = _to_float(latest.get("previous_close"))

    month_ref = _find_month_reference(rows, latest_date)
    month_px = _row_px(month_ref)

    year_ref = _find_year_reference(rows, latest_date)
    year_px = _row_px(year_ref)

    change_1d_pct = _calc_pct_from_two_values(latest_px, prev_px)
    if change_1d_pct is None:
        change_1d_pct = _to_float(latest.get("change_1d_pct"))

    mtd_pct = _calc_pct_from_two_values(latest_px, month_px)
    ytd_pct = _calc_pct_from_two_values(latest_px, year_px)

    return {
        "name": name,
        "ticker": instrument_code,
        "px_last": round(latest_px, 2) if latest_px is not None else "N/A",
        "change_1d": _fmt_pct_value(change_1d_pct, 1),
        "mtd": _fmt_pct_value(mtd_pct, 1),
        "ytd": _fmt_pct_value(ytd_pct, 1),
        "as_of": latest.get("as_of_date"),
        "source": latest.get("source") or "Supabase gcc_indices_history",
    }


def fetch_gcc_indices(today: datetime.date) -> list[dict]:
    rows = []
    for item in GCC_INDEX_CONFIG:
        row = _build_gcc_row(item["name"], item["code"], today)
        print(f"    {item['name']}: {row['px_last']} | {row['change_1d']} | {row['mtd']} | {row['ytd']}")
        rows.append(row)
    return rows


def fetch_section(ticker_map: dict, today: datetime.date) -> list[dict]:
    rows = []
    for name, sym in ticker_map.items():
        row = _fetch_market_row(name, sym, today)
        print(f"    {name}: {row['px_last']} | {row['change_1d']} | {row['mtd']} | {row['ytd']}")
        rows.append(row)
    return rows


def _find_row(rows: list[dict], name: str) -> Optional[dict]:
    for row in rows:
        if row["name"] == name:
            return row
    return None


def _build_derived_row(
    name: str,
    px_last: Optional[float],
    prev_px: Optional[float],
    month_base: Optional[float],
    year_base: Optional[float],
    source: str,
    digits: int = 2,
) -> dict:
    return {
        "name": name,
        "ticker": "DERIVED",
        "px_last": round(px_last, digits) if px_last is not None else "N/A",
        "change_1d": _fmt_pct_number(px_last, prev_px, 1),
        "mtd": _fmt_pct_number(px_last, month_base, 1),
        "ytd": _fmt_pct_number(px_last, year_base, 1),
        "as_of": datetime.date.today().strftime("%Y-%m-%d"),
        "source": source,
    }


def _download_close_series(sym: str, today: datetime.date):
    year_start = datetime.date(today.year, 1, 1)
    history_start = year_start - datetime.timedelta(days=20)
    return _get_series(sym, history_start)


def add_derived_rows(data: dict, today: datetime.date) -> None:
    comm = data.get("commodities", [])
    qar_rows = []

    qary = _download_close_series("QAR=X", today)
    eurusd = _download_close_series("EURUSD=X", today)
    gbpusd = _download_close_series("GBPUSD=X", today)
    usdcny = _download_close_series("CNY=X", today)
    goldusd = _download_close_series("GC=F", today)

    year_start = datetime.date(today.year, 1, 1)
    month_start = datetime.date(today.year, today.month, 1)

    def get_refs(series):
        if series is None or len(series) < 2:
            return None
        cur = _to_float(series.iloc[-1])
        prev = _to_float(series.iloc[-2])
        mtd = _last_value_before_or_on(series, month_start - datetime.timedelta(days=1))
        ytd = _last_value_before_or_on(series, year_start - datetime.timedelta(days=1))
        return cur, prev, mtd, ytd

    q = get_refs(qary)
    e = get_refs(eurusd)
    g = get_refs(gbpusd)
    c = get_refs(usdcny)
    au = get_refs(goldusd)

    if q:
        qar_rows.append(_build_derived_row(
            "USD/QAR", q[0], q[1], q[2], q[3], "Derived from Yahoo Finance QAR=X", 4
        ))

    if q and e:
        qar_rows.append(_build_derived_row(
            "EUR/QAR",
            e[0] * q[0],
            e[1] * q[1],
            e[2] * q[2] if e[2] is not None and q[2] is not None else None,
            e[3] * q[3] if e[3] is not None and q[3] is not None else None,
            "Derived from Yahoo Finance EURUSD=X and QAR=X",
            4
        ))

    if q and g:
        qar_rows.append(_build_derived_row(
            "GBP/QAR",
            g[0] * q[0],
            g[1] * q[1],
            g[2] * q[2] if g[2] is not None and q[2] is not None else None,
            g[3] * q[3] if g[3] is not None and q[3] is not None else None,
            "Derived from Yahoo Finance GBPUSD=X and QAR=X",
            4
        ))

    if q and c and c[0] not in (None, 0) and c[1] not in (None, 0):
        qar_rows.append(_build_derived_row(
            "CNY/QAR",
            q[0] / c[0],
            q[1] / c[1],
            (q[2] / c[2]) if q[2] not in (None, 0) and c[2] not in (None, 0) else None,
            (q[3] / c[3]) if q[3] not in (None, 0) and c[3] not in (None, 0) else None,
            "Derived from Yahoo Finance QAR=X and CNY=X",
            4
        ))

    data["qar_cross_rates"] = qar_rows

    if au and q:
        gold_qar = _build_derived_row(
            "Gold (QAR)",
            au[0] * q[0],
            au[1] * q[1],
            (au[2] * q[2]) if au[2] is not None and q[2] is not None else None,
            (au[3] * q[3]) if au[3] is not None and q[3] is not None else None,
            "Derived from Yahoo Finance GC=F and QAR=X",
            2
        )
        comm.append(gold_qar)

    data["commodities"] = [r for r in comm if r["name"] != "Gold (USD)"]


def validate_market_data(data: dict) -> List[str]:
    issues: List[str] = []

    qe = _find_row(data.get("gcc_indices", []), "Qatar QE Index")
    doha = _find_row(data.get("qatari_banks", []), "Doha")
    usdqar = _find_row(data.get("qar_cross_rates", []), "USD/QAR")

    if not qe or qe.get("px_last") == "N/A":
        issues.append("Qatar QE Index missing or invalid")

    if not doha or doha.get("px_last") == "N/A":
        issues.append("Doha Bank price missing or invalid")

    if usdqar and usdqar.get("change_1d") not in (None, "N/A"):
        try:
            raw = str(usdqar["change_1d"]).replace("%", "").strip()
            val = float(raw)
            if abs(val) > 0.5:
                issues.append(f"USD/QAR daily change suspicious: {usdqar['change_1d']}")
        except Exception:
            issues.append("USD/QAR daily change unparsable")

    return issues



def _parse_published_dt(item: dict) -> Optional[datetime.datetime]:
    raw = item.get("published", "")
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        try:
            dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc)
        except Exception:
            return None


def _is_recent_item(item: dict, now_utc: datetime.datetime, max_age_hours: int) -> bool:
    dt = _parse_published_dt(item)
    if dt is None:
        return False
    age = now_utc - dt
    return datetime.timedelta(0) <= age <= datetime.timedelta(hours=max_age_hours)


def _is_qatar_business_item(item: dict) -> bool:
    blob = f"{item.get('title', '')} {item.get('summary', '')}".lower()
    if "qatar" not in blob:
        return False
    if any(x in blob for x in QATAR_NEWS_EXCLUDE_KEYWORDS):
        return False
    return any(k in blob for k in QATAR_NEWS_REQUIRED_KEYWORDS)


def _filter_recent_qatar_business_news(items: list[dict]) -> list[dict]:
    now_utc = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    out = []
    for item in items:
        if not _is_recent_item(item, now_utc, QATAR_NEWS_MAX_AGE_HOURS):
            continue
        if not _is_qatar_business_item(item):
            continue
        out.append(item)
    out.sort(key=lambda x: _parse_published_dt(x) or datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc), reverse=True)
    return out


def _fetch_rss_url(url: str, timeout: int = 20):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/rss+xml, application/xml, text/xml, application/atom+xml, */*",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.content


def _google_news_rss_url(query: str) -> str:
    encoded = quote(query, safe="")
    return f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"


def _parse_feed_items(feed_cfg: dict) -> list[dict]:
    source = feed_cfg["source"]
    url = feed_cfg["url"]
    max_items = int(feed_cfg.get("max", 10))
    try:
        raw = _fetch_rss_url(url)
        feed = feedparser.parse(raw)
        print(f"    RSS {source} entries: {len(feed.entries)}")
        items = []
        for entry in feed.entries[:max_items]:
            title = _clean_text(entry.get("title", ""))
            summary = _clean_text(getattr(entry, "summary", ""))
            link = entry.get("link", "")
            published = entry.get("published", "")
            if not title:
                continue
            items.append({
                "source": source,
                "title": title,
                "summary": summary[:500],
                "link": link,
                "published": published,
            })
        return items
    except Exception as e:
        print(f"[WARN] RSS {source}: {e}")
        return []


def _parse_google_news_query(query_cfg: dict) -> list[dict]:
    source = query_cfg["source"]
    query = query_cfg["query"]
    max_items = int(query_cfg.get("max", 10))
    url = _google_news_rss_url(query)
    try:
        raw = _fetch_rss_url(url)
        feed = feedparser.parse(raw)
        print(f"    Google News fallback {source} entries: {len(feed.entries)}")
        items = []
        for entry in feed.entries[:max_items]:
            title = _clean_text(entry.get("title", ""))
            summary = _clean_text(getattr(entry, "summary", ""))
            link = entry.get("link", "")
            published = entry.get("published", "")
            if not title:
                continue
            items.append({
                "source": source,
                "title": title,
                "summary": summary[:500],
                "link": link,
                "published": published,
            })
        return items
    except Exception as e:
        print(f"[WARN] Google News fallback {source}: {e}")
        return []


def fetch_news(feed_list: list[dict]) -> list[dict]:
    items = []
    for feed_cfg in feed_list:
        items.extend(_parse_feed_items(feed_cfg))
    return items


def fetch_qatar_news() -> list[dict]:
    items = dedupe_news(fetch_news(NEWS_FEEDS["qatar"]))
    items = _filter_recent_qatar_business_news(items)
    print(f"    Qatar primary recent business items found: {len(items)}")

    if len(items) >= QATAR_NEWS_TARGET_COUNT:
        return items[:QATAR_NEWS_TARGET_COUNT]

    for query_cfg in QATAR_NEWS_FALLBACK_QUERIES:
        items.extend(_parse_google_news_query(query_cfg))
        items = dedupe_news(items)
        items = _filter_recent_qatar_business_news(items)
        print(f"    Qatar recent business items after fallback '{query_cfg['source']}': {len(items)}")
        if len(items) >= QATAR_NEWS_TARGET_COUNT:
            break

    return items[:QATAR_NEWS_TARGET_COUNT]


def dedupe_news(items: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for item in items:
        key = re.sub(r"[^a-z0-9]+", "", item.get("title", "").lower())[:120]
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def ensure_min_news(items: list[dict], count: int, fallback_source: str) -> list[dict]:
    out = list(items)

    placeholders = [
        {
            "source": fallback_source,
            "title": "Qatar business source temporarily unavailable",
            "summary": "The source feed returned no usable article in this cycle.",
            "link": "",
            "published": "",
        },
        {
            "source": fallback_source,
            "title": "Qatar corporate news feed refresh pending",
            "summary": "The workflow will retry the same approved source on the next run.",
            "link": "",
            "published": "",
        },
        {
            "source": fallback_source,
            "title": "Qatar market coverage awaiting source update",
            "summary": "Approved publisher feed did not return enough items for this report cycle.",
            "link": "",
            "published": "",
        },
        {
            "source": fallback_source,
            "title": "Qatar economy headline stream incomplete",
            "summary": "Only approved publisher sources are allowed for this section.",
            "link": "",
            "published": "",
        },
    ]

    i = 0
    while len(out) < count and i < len(placeholders):
        out.append(placeholders[i])
        i += 1

    return out[:count]


def _fallback_summarise_news(raw_items: list[dict], count: int) -> list[dict]:
    fallback = []
    for item in raw_items[:count]:
        source = item.get("source", "Feed")
        title = item.get("title", "")[:120]
        summary = item.get("summary", "")[:240]

        blob = f"{title.lower()} {summary.lower()}"
        metric = source.upper()[:8] if source else "NEWS"
        metric_label = "Source"

        if "oil" in blob or "gas" in blob or "energy" in blob:
            metric = "ENERGY"
            metric_label = "Sector"
        elif "bank" in blob or "qnb" in blob or "qib" in blob or "cbq" in blob:
            metric = "BANK"
            metric_label = "Sector"
        elif "tax" in blob or "policy" in blob:
            metric = "POLICY"
            metric_label = "Theme"
        elif "qatar" in blob:
            metric = "QATAR"
            metric_label = "Domestic"

        fallback.append({
            "headline": title or "Market update",
            "summary": summary or "Latest development relevant to markets.",
            "source": source,
            "url": item.get("link", ""),
            "metric": metric,
            "metric_label": metric_label,
        })

    while len(fallback) < count:
        fallback.append({
            "headline": "Market update",
            "summary": "Latest development relevant to markets.",
            "source": "Feed",
            "url": "",
            "metric": "NEWS",
            "metric_label": "Signal",
        })

    return fallback[:count]


def summarise_news(raw_items: list[dict], scope: str, count: int) -> list[dict]:
    if not raw_items:
        return []

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[WARN] ANTHROPIC_API_KEY not set, using fallback summarisation.")
        return _fallback_summarise_news(raw_items, count)

    client = anthropic.Anthropic(api_key=api_key)

    headlines_txt = "\n".join(
        f"[{i['source']}] {i['title']} — {i['summary']} (URL: {i['link']})"
        for i in raw_items
    )

    system = (
        "You are a financial news editor for a Gulf bank daily market intelligence report. "
        "Return only valid JSON. Select the most relevant stories and produce clean metric boxes."
    )

    prompt = f"""
From the following {scope} news items, select the {count} most relevant stories.

Return a JSON array of exactly {count} objects.

Each object must contain exactly these keys:
- headline
- summary
- source
- url
- metric
- metric_label

Rules:
- headline maximum 10 words
- summary maximum 40 words
- metric must be meaningful, short, and never just a dash unless absolutely impossible
- metric_label must explain the metric briefly
- no markdown fences
- no preamble

News:
{headlines_txt}
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1600,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        parsed = json.loads(text)

        if not isinstance(parsed, list):
            raise ValueError("Claude did not return a list")

        cleaned = []
        for item in parsed[:count]:
            cleaned.append({
                "headline": item.get("headline", "")[:120] or "Market update",
                "summary": item.get("summary", "")[:240] or "Latest development relevant to markets.",
                "source": item.get("source", "") or "Feed",
                "url": item.get("url", ""),
                "metric": item.get("metric", "")[:16] or "NEWS",
                "metric_label": item.get("metric_label", "")[:32] or "Signal",
            })

        while len(cleaned) < count:
            cleaned.append({
                "headline": "Market update",
                "summary": "Latest development relevant to markets.",
                "source": "Feed",
                "url": "",
                "metric": "NEWS",
                "metric_label": "Signal",
            })

        return cleaned[:count]

    except Exception as e:
        print(f"[WARN] Claude summarisation failed ({scope}): {e}")
        return _fallback_summarise_news(raw_items, count)


def build_kpis(market_data: dict) -> list[dict]:
    def px(rows, name):
        row = _find_row(rows, name)
        return row.get("px_last", "N/A") if row else "N/A"

    def chg(rows, name):
        row = _find_row(rows, name)
        return row.get("change_1d", "N/A") if row else "N/A"

    def ytd(rows, name):
        row = _find_row(rows, name)
        return row.get("ytd", "N/A") if row else "N/A"

    sp_1d = chg(market_data.get("global_indices", []), "US S&P 500")
    uk_1d = chg(market_data.get("global_indices", []), "UK FTSE 100")

    eq_label = "Positive"
    if isinstance(sp_1d, str) and isinstance(uk_1d, str):
        if sp_1d.startswith("-") or uk_1d.startswith("-"):
            eq_label = "Mixed"

    brent_px = px(market_data.get("commodities", []), "Brent Crude")
    brent_ytd = ytd(market_data.get("commodities", []), "Brent Crude")
    gold_qar = px(market_data.get("commodities", []), "Gold (QAR)")
    gold_ytd = ytd(market_data.get("commodities", []), "Gold (QAR)")
    qse_px = px(market_data.get("gcc_indices", []), "Qatar QE Index")
    qse_1d = chg(market_data.get("gcc_indices", []), "Qatar QE Index")
    qse_ytd = ytd(market_data.get("gcc_indices", []), "Qatar QE Index")
    ust10_px = px(market_data.get("fixed_income", []), "UST 10-Year")
    ust10_ytd = ytd(market_data.get("fixed_income", []), "UST 10-Year")

    return [
        {
            "value": eq_label,
            "label": "Global Equities",
            "sublabel": f"US {sp_1d} · UK {uk_1d}",
        },
        {
            "value": f"${brent_px}",
            "label": "Brent Crude",
            "sublabel": f"{brent_ytd} Year-to-Date",
        },
        {
            "value": f"{gold_qar:,}" if isinstance(gold_qar, float) else str(gold_qar),
            "label": "Gold (QAR)",
            "sublabel": f"{gold_ytd} YTD · Safe-haven demand",
        },
        {
            "value": f"{qse_px:,}" if isinstance(qse_px, float) else str(qse_px),
            "label": "QSE Index",
            "sublabel": f"{qse_1d} today · {qse_ytd} YTD",
        },
        {
            "value": f"{ust10_px}%",
            "label": "UST 10Y Yield",
            "sublabel": f"{ust10_ytd} YTD · Rising yields",
        },
        {
            "value": "4.50%",
            "label": "QCB Sukuk Yield",
            "sublabel": "QR3bn · 2.7x oversubscribed",
        },
    ]


def run() -> dict:
    today = datetime.date.today()
    cfg = CONFIG
    generated_at_utc = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    data = {
        "config": cfg,
        "generated_at": generated_at_utc,
        "generated_display_time": cfg.get("delivery_time_ast", "07:00") + " AST",
    }

    section_map = {
        "global_indices": GLOBAL_INDICES,
        "spot_currency": SPOT_CURRENCY,
        "qatari_banks": QATARI_BANKS,
        "commodities": COMMODITIES,
        "fixed_income": FIXED_INCOME,
    }

    print("▶ Fetching market data ...")
    for section, tickers in section_map.items():
        if not cfg["sections"].get(section, True):
            data[section] = []
            continue
        print(f"  · {section}")
        data[section] = fetch_section(tickers, today)

    if cfg["sections"].get("gcc_indices", True):
        print("  · gcc_indices")
        data["gcc_indices"] = fetch_gcc_indices(today)
    else:
        data["gcc_indices"] = []

    if cfg["sections"].get("qar_cross_rates", True):
        add_derived_rows(data, today)
    else:
        data["qar_cross_rates"] = []

    if cfg["sections"].get("global_news", True):
        print("  · global news")
        raw_global = dedupe_news(fetch_news(NEWS_FEEDS["global"]))
        raw_global = ensure_min_news(raw_global, 6, "Reuters/Bloomberg")
        data["global_news"] = summarise_news(raw_global, "regional & global", 6)
    else:
        data["global_news"] = []

    if cfg["sections"].get("qatar_news", True):
        print("  · qatar news")
        raw_qatar = fetch_qatar_news()
        print(f"    Qatar recent business items usable: {len(raw_qatar)}")
        if raw_qatar:
            data["qatar_news"] = summarise_news(raw_qatar, "qatar", min(QATAR_NEWS_TARGET_COUNT, len(raw_qatar)))
        else:
            data["qatar_news"] = []
    else:
        data["qatar_news"] = []

    data["kpis"] = build_kpis(data)

    validation_issues = validate_market_data(data)
    data["validation_issues"] = validation_issues
    data["report_status"] = "ok" if not validation_issues else "needs_review"

    print("✓ Fetch complete.")
    if validation_issues:
        print("⚠ Validation issues found:")
        for issue in validation_issues:
            print(f"   - {issue}")

    return data


if __name__ == "__main__":
    result = run()
    with open("market_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    print("✓ Data written to market_data.json")

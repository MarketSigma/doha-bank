import json
import os
import re
import datetime
from typing import Optional

import feedparser
import yfinance as yf
import anthropic

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

GCC_INDICES = {
    "Qatar QE Index": "^QSI",
    "Saudi Tadawul": "^TASI.SR",
    "Dubai DFM": "^DFMGI",
    "Abu Dhabi ADX": "ADSMI.AD",
    "Kuwait Boursa": "^BKW",
    "Bahrain": "^BHSE",
}

SPOT_CURRENCY = {
    "USD Index": "DX-Y.NYB",
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "CHF/USD": "CHFUSD=X",
    "USD/JPY": "JPY=X",
    "CNY/USD": "CNYUSD=X",
}

QAR_CROSS = {
    "USD/QAR": "USDQAR=X",
    "EUR/QAR": "EURQAR=X",
    "GBP/QAR": "GBPQAR=X",
    "CHF/QAR": "CHFQAR=X",
    # CNY/QAR will be derived
}

QATARI_BANKS = {
    "Doha": "BRES.QA",
    "QNB": "QNBK.QA",
    "QIB": "QIBK.QA",
    "CBQ": "CBQK.QA",
    "QIIB": "QIIK.QA",
    "Al Rayan": "MARK.QA",
    "Dukhan": "DBIS.QA",
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
            "max": 8,
        },
        {
            "source": "Qatar Tribune",
            "url": "https://www.qatar-tribune.com/rss",
            "max": 8,
        },
    ],
}


def _to_float(val) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None


def _fmt_pct(current: Optional[float], base: Optional[float]) -> str:
    if current is None or base in (None, 0):
        return "N/A"
    pct = ((current - base) / base) * 100
    return f"{pct:+.1f}%"


def _history(sym: str, start: datetime.date):
    try:
        df = yf.download(
            sym,
            start=start.strftime("%Y-%m-%d"),
            auto_adjust=True,
            progress=False,
            threads=False,
        )
        if df is None or df.empty or "Close" not in df.columns:
            return None
        closes = df["Close"].dropna()
        if closes.empty:
            return None
        return closes
    except Exception as e:
        print(f"[WARN] history failed for {sym}: {e}")
        return None


def fetch_stats(name: str, sym: str, today: datetime.date) -> dict:
    year_start = datetime.date(today.year, 1, 1)
    month_start = datetime.date(today.year, today.month, 1)
    history_start = min(year_start, month_start) - datetime.timedelta(days=10)

    closes = _history(sym, history_start)
    if closes is None or len(closes) == 0:
        return {
            "name": name,
            "ticker": sym,
            "px_last": "N/A",
            "change_1d": "N/A",
            "mtd": "N/A",
            "ytd": "N/A",
            "source": "Yahoo Finance",
        }

    px_last = _to_float(closes.iloc[-1])
    px_prev = _to_float(closes.iloc[-2]) if len(closes) >= 2 else None

    month_base = None
    year_base = None

    for dt_idx, px in closes.items():
        try:
            if hasattr(dt_idx, "date"):
                d = dt_idx.date()
            else:
                d = datetime.datetime.strptime(str(dt_idx)[:10], "%Y-%m-%d").date()
        except Exception:
            continue

        fpx = _to_float(px)

        if year_base is None and d >= year_start:
            year_base = fpx
        if month_base is None and d >= month_start:
            month_base = fpx
        if year_base is not None and month_base is not None:
            break

    return {
        "name": name,
        "ticker": sym,
        "px_last": round(px_last, 2) if px_last is not None else "N/A",
        "change_1d": _fmt_pct(px_last, px_prev),
        "mtd": _fmt_pct(px_last, month_base),
        "ytd": _fmt_pct(px_last, year_base),
        "source": "Yahoo Finance",
    }


def fetch_section(ticker_map: dict, today: datetime.date) -> list[dict]:
    return [fetch_stats(name, sym, today) for name, sym in ticker_map.items()]


def _find_row(rows: list[dict], name: str) -> Optional[dict]:
    for row in rows:
        if row["name"] == name:
            return row
    return None


def add_derived_rows(data: dict) -> None:
    comm = data.get("commodities", [])
    qar = data.get("qar_cross_rates", [])
    spot = data.get("spot_currency", [])

    gold_usd = _find_row(comm, "Gold (USD)")
    usd_qar = _find_row(qar, "USD/QAR")

    # Gold (QAR) = Gold (USD) × USD/QAR
    if gold_usd and usd_qar:
        g = _to_float(gold_usd.get("px_last"))
        q = _to_float(usd_qar.get("px_last"))
        if g is not None and q is not None:
            comm.append({
                "name": "Gold (QAR)",
                "ticker": "DERIVED",
                "px_last": round(g * q, 2),
                "change_1d": gold_usd.get("change_1d", "N/A"),
                "mtd": gold_usd.get("mtd", "N/A"),
                "ytd": gold_usd.get("ytd", "N/A"),
                "source": "Derived from Yahoo Finance GC=F and USD/QAR",
            })

    # CNY/QAR = CNY/USD × USD/QAR
    cny_usd = _find_row(spot, "CNY/USD")
    if cny_usd and usd_qar:
        c = _to_float(cny_usd.get("px_last"))
        q = _to_float(usd_qar.get("px_last"))
        if c is not None and q is not None:
            qar.append({
                "name": "CNY/QAR",
                "ticker": "DERIVED",
                "px_last": round(c * q, 4),
                "change_1d": cny_usd.get("change_1d", "N/A"),
                "mtd": cny_usd.get("mtd", "N/A"),
                "ytd": cny_usd.get("ytd", "N/A"),
                "source": "Derived from Yahoo Finance CNY/USD and USD/QAR",
            })

    # Remove Gold (USD) from final display
    data["commodities"] = [r for r in comm if r["name"] != "Gold (USD)"]


def fetch_news(feed_list: list[dict]) -> list[dict]:
    items = []
    for feed_cfg in feed_list:
        try:
            feed = feedparser.parse(feed_cfg["url"])
            for entry in feed.entries[: feed_cfg["max"]]:
                summary = getattr(entry, "summary", "")
                summary = re.sub(r"<[^>]+>", "", summary).strip()
                items.append({
                    "source": feed_cfg["source"],
                    "title": entry.get("title", ""),
                    "summary": summary[:400],
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                })
        except Exception as e:
            print(f"[WARN] RSS {feed_cfg['source']}: {e}")
    return items


def summarise_news(raw_items: list[dict], scope: str, count: int) -> list[dict]:
    if not raw_items:
        return []

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    headlines_txt = "\n".join(
        f"[{i['source']}] {i['title']} — {i['summary']} (URL: {i['link']})"
        for i in raw_items
    )

    system = (
        "You are a financial news editor for a Gulf bank's daily Market Intelligence report. "
        "Select the most market-relevant stories. Return ONLY valid JSON."
    )

    prompt = f"""
From the following {scope} news headlines, select the {count} most market-relevant stories.
For each, return a JSON object with these exact keys:
headline, summary, source, url, metric, metric_label

Rules:
- headline max 10 words
- summary max 40 words
- metric max 8 characters
- return exactly {count} objects
- no markdown fences
- no extra keys

Headlines:
{headlines_txt}
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1400,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text)
    except Exception as e:
        print(f"[WARN] Claude summarisation failed ({scope}): {e}")
        return [
            {
                "headline": item["title"][:60],
                "summary": item["summary"][:120],
                "source": item["source"],
                "url": item["link"],
                "metric": "—",
                "metric_label": "",
            }
            for item in raw_items[:count]
        ]


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
    eq_label = "Positive" if isinstance(sp_1d, str) and sp_1d.startswith("+") else "Mixed"

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
            "sublabel": f"US {sp_1d} · UK {chg(market_data.get('global_indices', []), 'UK FTSE 100')}",
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
    data = {"config": cfg, "generated_at": datetime.datetime.utcnow().isoformat()}

    section_map = {
        "global_indices": GLOBAL_INDICES,
        "gcc_indices": GCC_INDICES,
        "spot_currency": SPOT_CURRENCY,
        "qar_cross_rates": QAR_CROSS,
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

    add_derived_rows(data)

    if cfg["sections"].get("global_news", True):
        print("  · global news")
        raw_global = fetch_news(NEWS_FEEDS["global"])
        data["global_news"] = summarise_news(raw_global, "regional & global", 6)

    if cfg["sections"].get("qatar_news", True):
        print("  · qatar news")
        raw_qatar = fetch_news(NEWS_FEEDS["qatar"])
        data["qatar_news"] = summarise_news(raw_qatar, "qatar", 4)

    data["kpis"] = build_kpis(data)
    print("✓ Fetch complete.")
    return data


if __name__ == "__main__":
    result = run()
    with open("market_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    print("✓ Data written to market_data.json")

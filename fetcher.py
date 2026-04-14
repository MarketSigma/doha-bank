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
    # CNY/QAR derived later
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
    "qatar_primary": [
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
    "qatar_fallback": [
        {
            "source": "Google News",
            "url": "https://news.google.com/rss/search?q=Qatar+business+OR+Qatar+economy&hl=en-US&gl=US&ceid=US:en",
            "max": 10,
        }
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


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _safe_history(sym: str, start: datetime.date):
    try:
        ticker = yf.Ticker(sym)
        df = ticker.history(
            start=start.strftime("%Y-%m-%d"),
            auto_adjust=True,
            actions=False,
        )
        if df is None or df.empty:
            return None
        if "Close" not in df.columns:
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

    closes = _safe_history(sym, history_start)
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

    try:
        px_last = _to_float(closes.iloc[-1])
        px_prev = _to_float(closes.iloc[-2]) if len(closes) >= 2 else None
    except Exception:
        px_last = None
        px_prev = None

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
    rows = []
    for name, sym in ticker_map.items():
        row = fetch_stats(name, sym, today)
        print(f"    {name}: {row['px_last']} | {row['change_1d']} | {row['mtd']} | {row['ytd']}")
        rows.append(row)
    return rows


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
            gold_qar = round(g * q, 2)
            comm.append({
                "name": "Gold (QAR)",
                "ticker": "DERIVED",
                "px_last": gold_qar,
                "change_1d": gold_usd.get("change_1d", "N/A"),
                "mtd": gold_usd.get("mtd", "N/A"),
                "ytd": gold_usd.get("ytd", "N/A"),
                "source": "Derived from Yahoo Finance GC=F and USD/QAR",
            })
            print(f"    Gold (QAR) derived: {gold_qar}")

    # CNY/QAR = CNY/USD × USD/QAR
    cny_usd = _find_row(spot, "CNY/USD")
    if cny_usd and usd_qar:
        c = _to_float(cny_usd.get("px_last"))
        q = _to_float(usd_qar.get("px_last"))
        if c is not None and q is not None:
            cny_qar = round(c * q, 4)
            qar.append({
                "name": "CNY/QAR",
                "ticker": "DERIVED",
                "px_last": cny_qar,
                "change_1d": cny_usd.get("change_1d", "N/A"),
                "mtd": cny_usd.get("mtd", "N/A"),
                "ytd": cny_usd.get("ytd", "N/A"),
                "source": "Derived from Yahoo Finance CNY/USD and USD/QAR",
            })
            print(f"    CNY/QAR derived: {cny_qar}")

    # Remove Gold (USD) from final display
    data["commodities"] = [r for r in comm if r["name"] != "Gold (USD)"]


def fetch_news(feed_list: list[dict]) -> list[dict]:
    items = []
    for feed_cfg in feed_list:
        try:
            feed = feedparser.parse(feed_cfg["url"])
            for entry in feed.entries[: feed_cfg["max"]]:
                title = _clean_text(entry.get("title", ""))
                summary = _clean_text(getattr(entry, "summary", ""))
                link = entry.get("link", "")
                published = entry.get("published", "")

                if not title:
                    continue

                items.append({
                    "source": feed_cfg["source"],
                    "title": title,
                    "summary": summary[:500],
                    "link": link,
                    "published": published,
                })
        except Exception as e:
            print(f"[WARN] RSS {feed_cfg['source']}: {e}")
    return items


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


def fetch_qatar_news() -> list[dict]:
    """
    Primary source: Peninsula + Qatar Tribune RSS
    Fallback source: Google News RSS for Qatar business/economy
    """
    primary = dedupe_news(fetch_news(NEWS_FEEDS["qatar_primary"]))
    if len(primary) >= 4:
        print(f"    Qatar primary news found: {len(primary)}")
        return primary[:8]

    print(f"[WARN] Qatar primary news insufficient: {len(primary)}. Using fallback.")
    fallback = dedupe_news(fetch_news(NEWS_FEEDS["qatar_fallback"]))

    merged = dedupe_news(primary + fallback)

    if not merged:
        print("[WARN] Qatar news completely unavailable, injecting fallback placeholders.")
        merged = [
            {
                "source": "System Fallback",
                "title": "Qatar economic activity updates pending live source refresh",
                "summary": "Primary and fallback Qatar news feeds returned no usable items in this cycle.",
                "link": "",
                "published": "",
            },
            {
                "source": "System Fallback",
                "title": "Qatar business headlines temporarily unavailable",
                "summary": "Dashboard and PDF fallback card inserted to avoid empty news section.",
                "link": "",
                "published": "",
            },
            {
                "source": "System Fallback",
                "title": "Qatar market news feed reconnect in progress",
                "summary": "Live Qatar-specific sources did not return articles during this run.",
                "link": "",
                "published": "",
            },
            {
                "source": "System Fallback",
                "title": "Qatar macro and corporate news source retry scheduled",
                "summary": "The next workflow run will attempt source refresh automatically.",
                "link": "",
                "published": "",
            },
        ]

    return merged[:8]


def summarise_news(raw_items: list[dict], scope: str, count: int) -> list[dict]:
    if not raw_items:
        return []

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    headlines_txt = "\n".join(
        f"[{i['source']}] {i['title']} — {i['summary']} (URL: {i['link']})"
        for i in raw_items
    )

    system = (
        "You are a financial news editor for a Gulf bank daily market intelligence report. "
        "Return only valid JSON. Select the most relevant stories and produce clean metric boxes."
    )

    prompt = f"""
From the following {scope} news items, select the {count} most market-relevant stories.

Return a JSON array of exactly {count} objects.

Each object must contain exactly these keys:
- headline
- summary
- source
- url
- metric
- metric_label

Rules:
- headline: maximum 10 words
- summary: maximum 40 words
- source: publication/source name
- url: original article URL
- metric: must be a short meaningful signal, for example HIGH, +6%, $107.74, 2.7x, 1 Apr, Tax, QIB, Oil
- metric_label: short explanation of the metric
- Do NOT use "—" unless absolutely impossible
- No markdown fences
- No preamble

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
                "headline": item.get("headline", "")[:120],
                "summary": item.get("summary", "")[:240],
                "source": item.get("source", ""),
                "url": item.get("url", ""),
                "metric": item.get("metric", "")[:16] or "NEWS",
                "metric_label": item.get("metric_label", "")[:32] or "Signal",
            })

        while len(cleaned) < count:
            cleaned.append({
                "headline": raw_items[len(cleaned)].get("title", "Market update")[:120] if len(raw_items) > len(cleaned) else "Market update",
                "summary": raw_items[len(cleaned)].get("summary", "Latest development relevant to markets.")[:240] if len(raw_items) > len(cleaned) else "Latest development relevant to markets.",
                "source": raw_items[len(cleaned)].get("source", "Feed") if len(raw_items) > len(cleaned) else "Feed",
                "url": raw_items[len(cleaned)].get("link", "") if len(raw_items) > len(cleaned) else "",
                "metric": "NEWS",
                "metric_label": "Signal",
            })

        return cleaned[:count]

    except Exception as e:
        print(f"[WARN] Claude summarisation failed ({scope}): {e}")

        fallback = []
        for item in raw_items[:count]:
            source = item.get("source", "Feed")
            title = item.get("title", "")[:120]
            summary = item.get("summary", "")[:240]

            metric = source.upper()[:8] if source else "NEWS"
            metric_label = "Source"

            # light business-specific metric heuristics
            title_lower = title.lower()
            summary_lower = summary.lower()
            blob = f"{title_lower} {summary_lower}"

            if "oil" in blob or "brent" in blob or "wti" in blob:
                metric = "OIL"
                metric_label = "Energy"
            elif "bank" in blob or "qnb" in blob or "qib" in blob or "cbq" in blob:
                metric = "BANK"
                metric_label = "Financials"
            elif "tax" in blob:
                metric = "TAX"
                metric_label = "Policy"
            elif "qatar" in blob:
                metric = "QATAR"
                metric_label = "Domestic"

            fallback.append({
                "headline": title,
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
        raw_global = dedupe_news(fetch_news(NEWS_FEEDS["global"]))
        data["global_news"] = summarise_news(raw_global, "regional & global", 6)

    if cfg["sections"].get("qatar_news", True):
        print("  · qatar news")
        raw_qatar = fetch_qatar_news()
        print(f"    Qatar raw items available: {len(raw_qatar)}")
        data["qatar_news"] = summarise_news(raw_qatar, "qatar", 4)

    data["kpis"] = build_kpis(data)
    print("✓ Fetch complete.")
    return data


if __name__ == "__main__":
    result = run()
    with open("market_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    print("✓ Data written to market_data.json")

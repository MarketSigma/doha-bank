"""
Doha Bank – Market Intelligence Daily Fetcher
=============================================
Pulls all market data + news, summarises via Claude API, returns
a structured JSON payload ready for PDF rendering.

Sources:
  Market data  → yfinance (Yahoo Finance)
  Global news  → Reuters RSS + Bloomberg RSS
  Qatar news   → The Peninsula RSS + Qatar Tribune RSS
  AI summary   → Anthropic Claude API
"""

import json
import os
import re
import datetime
import feedparser
import yfinance as yf
import anthropic

# ─────────────────────────────────────────────
# CONFIG  –  toggle sections per client here
# ─────────────────────────────────────────────
CONFIG = {
    "client_name":        "Doha Bank",
    "report_date":        datetime.date.today().strftime("%d %B %Y"),
    "delivery_time_ast":  "07:00",
    "sections": {
        "global_indices":   True,
        "gcc_indices":      True,
        "spot_currency":    True,
        "qar_cross_rates":  True,
        "fixed_income":     True,
        "qatari_banks":     True,
        "commodities":      True,
        "global_news":      True,
        "qatar_news":       True,
    }
}

# ─────────────────────────────────────────────
# TICKERS
# ─────────────────────────────────────────────
GLOBAL_INDICES = {
    "US S&P 500":    "^GSPC",
    "UK FTSE 100":   "^FTSE",
    "Japan Nikkei":  "^N225",
    "Germany DAX":   "^GDAXI",
    "Hong Kong HSI": "^HSI",
    "India Sensex":  "^BSESN",
}

GCC_INDICES = {
    "Qatar QE Index":  "^QSI",
    "Saudi Tadawul":   "^TASI.SR",
    "Dubai DFM":       "^DFMGI",
    "Abu Dhabi ADX":   "ADSMI.AD",
    "Kuwait Boursa":   "^BKW",
    "Bahrain":         "^BHSE",
}

SPOT_CURRENCY = {
    "USD Index":   "DX-Y.NYB",
    "EUR/USD":     "EURUSD=X",
    "GBP/USD":     "GBPUSD=X",
    "CHF/USD":     "CHFUSD=X",
    "USD/JPY":     "JPY=X",
    "CNY/USD":     "CNYUSD=X",
}

QAR_CROSS = {
    "USD/QAR":  "USDQAR=X",
    "EUR/QAR":  "EURQAR=X",
    "GBP/QAR":  "GBPQAR=X",
    "CHF/QAR":  "CHFQAR=X",
    "CNY/QAR":  "CNYQAR=X",
}

QATARI_BANKS = {
    "Doha":     "BRES.QA",
    "QNB":      "QNBK.QA",
    "QIB":      "QIBK.QA",
    "CBQ":      "CBQK.QA",
    "QIIB":     "QIIK.QA",
    "Al Rayan": "MARK.QA",
    "Dukhan":   "DBIS.QA",
    "Ahli":     "ABQK.QA",
    "Lesha":    "IQCD.QA",   # proxy — update if different listing
}

COMMODITIES = {
    "Brent Crude": "BZ=F",
    "Gold (QAR)":  "GC=F",
    "Silver":      "SI=F",
    "LNG JP/KR":   "NG=F",   # Henry Hub proxy – closest public LNG
}

FIXED_INCOME = {
    "UST 5-Year":  "^FVX",
    "UST 10-Year": "^TNX",
}

# ─────────────────────────────────────────────
# NEWS RSS FEEDS
# ─────────────────────────────────────────────
NEWS_FEEDS = {
    "global": [
        {
            "source": "Reuters",
            "url":    "https://feeds.reuters.com/reuters/businessNews",
            "max":    6,
        },
        {
            "source": "Bloomberg",
            "url":    "https://feeds.bloomberg.com/markets/news.rss",
            "max":    6,
        },
    ],
    "qatar": [
        {
            "source": "The Peninsula",
            "url":    "https://thepeninsulaqatar.com/rss/business",
            "max":    5,
        },
        {
            "source": "Qatar Tribune",
            "url":    "https://www.qatar-tribune.com/rss",
            "max":    5,
        },
    ],
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _pct(current, prev):
    """Return 1D percentage change string e.g. '+1.2%'"""
    if prev and prev != 0:
        pct = ((current - prev) / prev) * 100
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.1f}%"
    return "N/A"


def fetch_ticker(ticker_map: dict, period: str = "2d") -> list[dict]:
    """Fetch a batch of tickers and return structured rows."""
    rows = []
    symbols = list(ticker_map.values())
    names   = list(ticker_map.keys())

    try:
        data = yf.download(
            symbols,
            period=period,
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        print(f"[WARN] yfinance batch download failed: {e}")
        data = {}

    for name, sym in zip(names, symbols):
        try:
            if len(symbols) == 1:
                closes = data["Close"].dropna()
            else:
                closes = data[sym]["Close"].dropna()

            if len(closes) < 1:
                raise ValueError("no data")

            px_last = round(float(closes.iloc[-1]), 2)
            px_prev = round(float(closes.iloc[-2]), 2) if len(closes) >= 2 else None
            change_1d = _pct(px_last, px_prev) if px_prev else "N/A"

            rows.append({
                "name":      name,
                "ticker":    sym,
                "px_last":   px_last,
                "change_1d": change_1d,
            })
        except Exception as ex:
            print(f"[WARN] {name} ({sym}): {ex}")
            rows.append({
                "name":      name,
                "ticker":    sym,
                "px_last":   "N/A",
                "change_1d": "N/A",
            })

    return rows


def fetch_ytd(ticker_map: dict) -> dict:
    """Return {name: ytd_pct_str} using Jan 1 as base."""
    ytd_map = {}
    start = f"{datetime.date.today().year}-01-01"

    for name, sym in ticker_map.items():
        try:
            df = yf.download(sym, start=start, auto_adjust=True,
                             progress=False, threads=False)
            closes = df["Close"].dropna()
            if len(closes) >= 2:
                ytd_map[name] = _pct(float(closes.iloc[-1]), float(closes.iloc[0]))
            else:
                ytd_map[name] = "N/A"
        except Exception as ex:
            print(f"[WARN] YTD {name}: {ex}")
            ytd_map[name] = "N/A"

    return ytd_map


def enrich_ytd(rows: list[dict], ytd_map: dict) -> list[dict]:
    for row in rows:
        row["ytd"] = ytd_map.get(row["name"], "N/A")
    return rows


def fetch_news(feed_list: list[dict]) -> list[dict]:
    """Parse RSS feeds and return raw headline items."""
    items = []
    for feed_cfg in feed_list:
        try:
            feed = feedparser.parse(feed_cfg["url"])
            for entry in feed.entries[: feed_cfg["max"]]:
                summary = getattr(entry, "summary", "")
                # strip HTML tags
                summary = re.sub(r"<[^>]+>", "", summary).strip()
                items.append({
                    "source":    feed_cfg["source"],
                    "title":     entry.get("title", ""),
                    "summary":   summary[:400],
                    "link":      entry.get("link", ""),
                    "published": entry.get("published", ""),
                })
        except Exception as e:
            print(f"[WARN] RSS {feed_cfg['source']}: {e}")
    return items


def summarise_news(raw_items: list[dict], scope: str) -> list[dict]:
    """
    Send raw headlines to Claude API.
    Returns a list of curated news cards with citation.
    scope: 'global' | 'qatar'
    """
    if not raw_items:
        return []

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    headlines_txt = "\n".join(
        f"[{i['source']}] {i['title']} — {i['summary']} (URL: {i['link']})"
        for i in raw_items
    )

    system = (
        "You are a financial news editor for a Gulf bank's daily Market Intelligence report. "
        "Your job is to select and summarise the most market-relevant news stories. "
        "Always cite the source. Return ONLY valid JSON, no markdown fences."
    )

    prompt = f"""
From the following {scope} news headlines, select the 4 most market-relevant stories.
For each, return a JSON object with these exact keys:
  headline   – concise title (max 10 words)
  summary    – 2-sentence summary in financial language (max 40 words)
  source     – name of publication
  url        – original article URL
  metric     – a key number or label to highlight (e.g. "+6%", "HIGH", "$107.74"). Max 8 chars.
  metric_label – short label for the metric (e.g. "LME Al Futures 1D", "Global Risk Level")

Return a JSON array of exactly 4 objects. No extra keys. No preamble.

Headlines:
{headlines_txt}
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text)
    except Exception as e:
        print(f"[WARN] Claude summarisation failed ({scope}): {e}")
        # fallback: return raw items trimmed
        return [
            {
                "headline":     item["title"][:60],
                "summary":      item["summary"][:120],
                "source":       item["source"],
                "url":          item["link"],
                "metric":       "—",
                "metric_label": "",
            }
            for item in raw_items[:4]
        ]


# ─────────────────────────────────────────────
# HEADLINE KPIs  (top banner)
# ─────────────────────────────────────────────

def build_kpis(market_data: dict) -> list[dict]:
    """Return the 6 top-banner KPI cards matching the Doha Bank layout."""

    def _get_px(section_rows, name):
        for r in section_rows:
            if r["name"] == name:
                return r.get("px_last", "N/A")
        return "N/A"

    # Global equities sentiment: positive if S&P > 0 1D, else Mixed/Negative
    sp_row = next((r for r in market_data["global_indices"] if "S&P" in r["name"]), None)
    sp_1d  = sp_row["change_1d"] if sp_row else "N/A"
    if sp_1d == "N/A":
        eq_label = "Mixed"
    elif sp_1d.startswith("+"):
        eq_label = "Positive"
    else:
        eq_label = "Mixed"

    brent_px  = _get_px(market_data["commodities"], "Brent Crude")
    gold_px   = _get_px(market_data["commodities"], "Gold (QAR)")
    qse_row   = next((r for r in market_data["gcc_indices"] if "Qatar" in r["name"]), None)
    qse_px    = qse_row["px_last"] if qse_row else "N/A"
    qse_1d    = qse_row["change_1d"] if qse_row else "N/A"
    ust10_row = next((r for r in market_data["fixed_income"] if "10" in r["name"]), None)
    ust10_px  = ust10_row["px_last"] if ust10_row else "N/A"
    ust10_ytd = ust10_row.get("ytd", "N/A")

    return [
        {
            "value":    eq_label,
            "label":    "Global Equities",
            "sublabel": f"US {sp_1d} YTD · UK {_get_sp_ytd(market_data, 'UK FTSE 100')} YTD",
        },
        {
            "value":    f"${brent_px}",
            "label":    "Brent Crude",
            "sublabel": f"+{market_data['commodities'][0].get('ytd','N/A')} Year-to-Date",
        },
        {
            "value":    f"{gold_px:,}" if isinstance(gold_px, float) else gold_px,
            "label":    "Gold (QAR)",
            "sublabel": "Safe-haven demand",
        },
        {
            "value":    f"{qse_px:,}" if isinstance(qse_px, float) else str(qse_px),
            "label":    "QSE Index",
            "sublabel": f"{qse_1d} today",
        },
        {
            "value":    f"{ust10_px}%",
            "label":    "UST 10Y Yield",
            "sublabel": f"{ust10_ytd} YTD · Rising yields",
        },
        {
            "value":    "4.50%",           # QCB Sukuk – static until live feed available
            "label":    "QCB Sukuk Yield",
            "sublabel": "QR3bn · 2.7x oversubscribed",
        },
    ]


def _get_sp_ytd(market_data, name):
    for r in market_data.get("global_indices", []):
        if r["name"] == name:
            return r.get("ytd", "N/A")
    return "N/A"


# ─────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────

def run() -> dict:
    cfg = CONFIG
    data = {"config": cfg, "generated_at": datetime.datetime.utcnow().isoformat()}

    print("▶ Fetching market data …")

    # — Market data sections —
    all_tickers = {}
    section_map = {
        "global_indices":  GLOBAL_INDICES,
        "gcc_indices":     GCC_INDICES,
        "spot_currency":   SPOT_CURRENCY,
        "qar_cross_rates": QAR_CROSS,
        "qatari_banks":    QATARI_BANKS,
        "commodities":     COMMODITIES,
        "fixed_income":    FIXED_INCOME,
    }

    for section, tickers in section_map.items():
        if not cfg["sections"].get(section, True):
            data[section] = []
            continue

        print(f"  · {section}")
        rows = fetch_ticker(tickers)

        # YTD for selected sections
        if section in ("global_indices", "gcc_indices", "commodities", "fixed_income"):
            ytd_map = fetch_ytd(tickers)
            rows = enrich_ytd(rows, ytd_map)

        data[section] = rows

    # — News sections —
    if cfg["sections"].get("global_news", True):
        print("  · global news (RSS)")
        raw_global = fetch_news(NEWS_FEEDS["global"])
        print("  · summarising global news via Claude …")
        data["global_news"] = summarise_news(raw_global, "global")

    if cfg["sections"].get("qatar_news", True):
        print("  · qatar news (RSS)")
        raw_qatar = fetch_news(NEWS_FEEDS["qatar"])
        print("  · summarising Qatar news via Claude …")
        data["qatar_news"] = summarise_news(raw_qatar, "qatar")

    # — KPI banner —
    data["kpis"] = build_kpis(data)

    print("✓ Fetch complete.")
    return data


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    result = run()
    out_path = "market_data.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"✓ Data written to {out_path}")

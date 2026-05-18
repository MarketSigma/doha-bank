import json
import os
import re
import datetime
from email.utils import parsedate_to_datetime
from typing import Optional, List, Dict, Any

import feedparser
import anthropic
import requests


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


SUPABASE_TABLE = "market_indices_history"
EXPECTED_INSTRUMENT_COUNT = 39
STALE_DATA_WARNING_DAYS = 3
USD_QAR_SUSPICIOUS_MOVE_THRESHOLD = 10.0

QATAR_NEWS_TARGET_COUNT = 8
QATAR_NEWS_MIN_VALID_COUNT = 4
QATAR_NEWS_MAX_AGE_HOURS = 24

GLOBAL_NEWS_TARGET_COUNT = 8
GLOBAL_NEWS_MAX_AGE_HOURS = 36

# ------------------------------------------------------------
# Global news — US + GCC editorial focus
# ------------------------------------------------------------
# Items that match at least one INCLUDE keyword AND no EXCLUDE
# keyword survive the relevance filter. The Claude summariser is
# also prompted to prioritise these themes when picking the top 6.
# ------------------------------------------------------------

GLOBAL_FOCUS_KEYWORDS = [
    # US macro / policy
    "fed", "federal reserve", "fomc", "powell", "rate cut", "rate hike",
    "interest rate", "treasury", "yield", "yields", "cpi", "ppi",
    "inflation", "deflation", "gdp", "nfp", "jobs report", "unemployment",
    "wall street", "s&p", "s&p 500", "dow", "nasdaq", "russell",
    "trump", "biden", "white house", "congress", "tariff", "tariffs",
    "sanctions", "sec",
    # GCC / MENA
    "saudi", "saudi arabia", "riyadh", "aramco", "saudi aramco",
    "pif", "vision 2030", "neom",
    "uae", "emirates", "dubai", "abu dhabi", "adnoc", "mubadala", "adia",
    "kuwait", "bahrain", "oman", "qatar",
    "gcc", "gulf", "gulf states", "mena", "middle east",
    "tadawul", "dfm", "adx",
    # Energy / OPEC
    "opec", "opec+", "oil", "brent", "wti", "crude", "petroleum",
    "lng", "liquefied natural gas", "natural gas", "energy",
    # Geopolitics that moves Gulf markets
    "iran", "israel", "hormuz", "strait of hormuz", "houthi", "yemen",
    "russia", "ukraine", "china",
    # Markets / FX / banking
    "dollar", "dxy", "euro", "yen", "yuan", "renminbi",
    "goldman", "jpmorgan", "morgan stanley", "blackrock",
    "ipo", "merger", "acquisition", "sovereign wealth",
    # Commodities
    "gold", "silver", "commodity", "commodities",
]

GLOBAL_EXCLUDE_KEYWORDS = [
    "football", "soccer", "cricket", "tennis", "basketball", "volleyball",
    "golf", "league", "fifa", "world cup", "match", "player", "team",
    "school", "teacher", "health", "weather", "traffic",
    "entertainment", "celebrity", "movie", "film", "music", "album",
    "award show", "fashion", "lifestyle", "recipe", "horoscope",
]

# Brave Search fallback queries — used if the RSS feeds return too few
# items after relevance filtering.
GLOBAL_NEWS_BRAVE_QUERIES = [
    'US Fed Powell Treasury yields markets today',
    'Saudi Aramco UAE Dubai Abu Dhabi GCC business',
    'OPEC oil Brent crude price today',
    'Middle East Iran Israel oil gas markets',
    'Wall Street S&P 500 Nasdaq today',
    'US dollar DXY currency Federal Reserve',
]

QATAR_BUSINESS_KEYWORDS = [
    "business", "economy", "economic", "investment", "investor", "investors",
    "bank", "banking", "finance", "financial", "market", "markets", "stock",
    "stocks", "trade", "trading", "company", "companies", "corporate",
    "ipo", "bond", "bonds", "sukuk", "merger", "acquisition", "real estate",
    "energy", "gas", "lng", "qse", "profit", "earnings", "revenue", "growth",
    "central bank", "qcb", "fund", "funding", "project", "sector", "digital economy"
]

QATAR_EXCLUDE_KEYWORDS = [
    "football", "match", "league", "fifa", "world cup", "tennis", "basketball",
    "volleyball", "school", "teacher", "health", "traffic", "weather", "entertainment"
]

QATAR_NEWS_BRAVE_QUERIES = [
    'Qatar business economy investment bank finance market',
    'Qatar banking finance economy investment Doha business',
    'Qatar stock exchange QSE banks economy investment',
    'Qatar energy LNG economy investment business',
    'QatarEnergy gas North Field expansion project',
    'Qatar Investment Authority QIA sovereign wealth deal',
    'Qatar real estate construction infrastructure project',
    'Qatar Central Bank QCB monetary policy banking sector',
]




PRICE_RANGES = {
    "SPX": (5000, 9000),
    "FTSE100": (7000, 12000),
    "NIKKEI225": (30000, 80000),
    "DAX": (15000, 35000),
    "HSI": (15000, 40000),
    "NIFTY50": (15000, 35000),
    "QE": (8000, 15000),
    "TASI": (8000, 16000),
    "DFMGI": (3000, 9000),
    "FADGI": (7000, 13000),
    "BKA": (6000, 12000),
    "OMAN": (3000, 11000),
    "BHSEASI": (1500, 2500),
    "DXY": (80, 120),
    "EURUSD": (0.90, 1.30),
    "GBPUSD": (1.00, 1.60),
    "USDCHF": (0.50, 1.20),
    "USDJPY": (100, 220),
    "USDCNY": (5.50, 8.50),
    "USDQAR": (3.60, 3.70),
    "EURQAR": (3.20, 5.20),
    "GBPQAR": (4.00, 6.00),
    "CHFQAR": (3.00, 6.00),
    "CNYQAR": (0.40, 0.70),
    "UST5Y": (0.50, 8.00),
    "UST10Y": (0.50, 8.00),
    "QIBK": (10.00, 40.00),
    "CBQK": (2.00, 10.00),
    "QIIB": (5.00, 20.00),
    "DUBK": (1.00, 8.00),
    "DHBK": (1.00, 6.00),
    "ABQK": (1.00, 8.00),
    "QNBK": (10.00, 30.00),
    "MARK": (1.00, 6.00),
    "LESHA": (0.50, 5.00),
    "BRENT": (40, 150),
    "LNGJK": (0.10, 100.00),
    "SILVER": (10, 120),
    "GOLD": (1000, 7000),
}

MAX_REASONABLE_1D_PCT = {
    "default": 15.0,
    "USDQAR": 0.25,
    "EURQAR": 5.0,
    "GBPQAR": 5.0,
    "CHFQAR": 5.0,
    "CNYQAR": 5.0,
    "EURUSD": 5.0,
    "GBPUSD": 5.0,
    "USDCHF": 5.0,
    "USDJPY": 5.0,
    "USDCNY": 5.0,
    "UST5Y": 15.0,
    "UST10Y": 15.0,
    "DHBK": 10.0,
    "QNBK": 10.0,
    "QIBK": 10.0,
    "CBQK": 10.0,
    "QIIB": 10.0,
    "MARK": 10.0,
    "DUBK": 10.0,
    "ABQK": 10.0,
    "LESHA": 10.0,
    "LNGJK": 25.0,
}

QATAR_BUSINESS_PAGES = [
    "https://www.qatar-tribune.com/business",
    "https://thepeninsulaqatar.com/category/Qatar-Business",
]

REPORT_SECTION_TO_OUTPUT_KEY = {
    "GLOBAL INDICES": "global_indices",
    "GCC & REGIONAL INDICES": "gcc_indices",
    "SPOT CURRENCY": "spot_currency",
    "QAR CROSS RATES": "qar_cross_rates",
    "FIXED INCOME — UST YIELDS": "fixed_income",
    "FIXED INCOME - UST YIELDS": "fixed_income",
    "QATARI BANKS": "qatari_banks",
    "COMMODITIES & ENERGY": "commodities",
}


EXPECTED_INSTRUMENTS = [
    {"code": "SPX", "name": "US S&P 500", "symbol": "^GSPC", "report_section": "GLOBAL INDICES", "display_order": 1},
    {"code": "FTSE100", "name": "UK FTSE 100", "symbol": "^FTSE", "report_section": "GLOBAL INDICES", "display_order": 2},
    {"code": "NIKKEI225", "name": "Japan Nikkei", "symbol": "^N225", "report_section": "GLOBAL INDICES", "display_order": 3},
    {"code": "DAX", "name": "Germany DAX", "symbol": "^GDAXI", "report_section": "GLOBAL INDICES", "display_order": 4},
    {"code": "HSI", "name": "Hong Kong HSI", "symbol": "^HSI", "report_section": "GLOBAL INDICES", "display_order": 5},
    {"code": "NIFTY50", "name": "India Nifty 50", "symbol": "^NSEI", "report_section": "GLOBAL INDICES", "display_order": 6},

    {"code": "QE", "name": "Qatar QE Index", "symbol": "^GNRI.QA", "report_section": "GCC & REGIONAL INDICES", "display_order": 1},
    {"code": "TASI", "name": "Saudi Tadawul", "symbol": "^TASI.SR", "report_section": "GCC & REGIONAL INDICES", "display_order": 2},
    {"code": "DFMGI", "name": "Dubai DFM", "symbol": "DFMGI", "report_section": "GCC & REGIONAL INDICES", "display_order": 3},
    {"code": "FADGI", "name": "Abu Dhabi ADX", "symbol": "FADGI", "report_section": "GCC & REGIONAL INDICES", "display_order": 4},
    {"code": "BKA", "name": "Kuwait Boursa", "symbol": "BKA", "report_section": "GCC & REGIONAL INDICES", "display_order": 5},
    {"code": "OMAN", "name": "Oman", "symbol": "MSX30", "report_section": "GCC & REGIONAL INDICES", "display_order": 6},
    {"code": "BHSEASI", "name": "Bahrain", "symbol": "BHSEASI", "report_section": "GCC & REGIONAL INDICES", "display_order": 7},

    {"code": "DXY", "name": "USD Index", "symbol": "DXY", "report_section": "SPOT CURRENCY", "display_order": 1},
    {"code": "EURUSD", "name": "EUR/USD", "symbol": "EURUSD", "report_section": "SPOT CURRENCY", "display_order": 2},
    {"code": "GBPUSD", "name": "GBP/USD", "symbol": "GBPUSD", "report_section": "SPOT CURRENCY", "display_order": 3},
    {"code": "USDCHF", "name": "USD/CHF", "symbol": "USDCHF", "report_section": "SPOT CURRENCY", "display_order": 4},
    {"code": "USDJPY", "name": "USD/JPY", "symbol": "USDJPY", "report_section": "SPOT CURRENCY", "display_order": 5},
    {"code": "USDCNY", "name": "USD/CNY", "symbol": "USDCNY", "report_section": "SPOT CURRENCY", "display_order": 6},

    {"code": "USDQAR", "name": "USD/QAR", "symbol": "USDQAR", "report_section": "QAR CROSS RATES", "display_order": 1},
    {"code": "EURQAR", "name": "EUR/QAR", "symbol": "EURQAR", "report_section": "QAR CROSS RATES", "display_order": 2},
    {"code": "GBPQAR", "name": "GBP/QAR", "symbol": "GBPQAR", "report_section": "QAR CROSS RATES", "display_order": 3},
    {"code": "CHFQAR", "name": "CHF/QAR", "symbol": "CHFQAR", "report_section": "QAR CROSS RATES", "display_order": 4},
    {"code": "CNYQAR", "name": "CNY/QAR", "symbol": "CNYQAR", "report_section": "QAR CROSS RATES", "display_order": 5},

    {"code": "UST5Y", "name": "UST 5-Year", "symbol": "US5Y", "report_section": "FIXED INCOME — UST YIELDS", "display_order": 1},
    {"code": "UST10Y", "name": "UST 10-Year", "symbol": "US10Y", "report_section": "FIXED INCOME — UST YIELDS", "display_order": 2},

    {"code": "QIBK", "name": "QIB", "symbol": "QIBK.QA", "report_section": "QATARI BANKS", "display_order": 1},
    {"code": "CBQK", "name": "CBQ", "symbol": "CBQK.QA", "report_section": "QATARI BANKS", "display_order": 2},
    {"code": "QIIB", "name": "QIIB", "symbol": "QIIK.QA", "report_section": "QATARI BANKS", "display_order": 3},
    {"code": "DUBK", "name": "Dukhan", "symbol": "DUBK.QA", "report_section": "QATARI BANKS", "display_order": 4},
    {"code": "DHBK", "name": "Doha", "symbol": "DHBK.QA", "report_section": "QATARI BANKS", "display_order": 5},
    {"code": "ABQK", "name": "Ahli", "symbol": "ABQK.QA", "report_section": "QATARI BANKS", "display_order": 6},
    {"code": "QNBK", "name": "QNB", "symbol": "QNBK.QA", "report_section": "QATARI BANKS", "display_order": 7},
    {"code": "MARK", "name": "Al Rayan", "symbol": "MARK.QA", "report_section": "QATARI BANKS", "display_order": 8},
    {"code": "LESHA", "name": "Lesha", "symbol": "QFBQ.QA", "report_section": "QATARI BANKS", "display_order": 9},

    {"code": "BRENT", "name": "Brent Crude", "symbol": "BZ=F", "report_section": "COMMODITIES & ENERGY", "display_order": 1},
    {"code": "LNGJK", "name": "LNG JP/KR", "symbol": "JKM", "report_section": "COMMODITIES & ENERGY", "display_order": 2},
    {"code": "SILVER", "name": "Silver", "symbol": "XAGUSD", "report_section": "COMMODITIES & ENERGY", "display_order": 3},
    {"code": "GOLD", "name": "Gold", "symbol": "XAUUSD", "report_section": "COMMODITIES & ENERGY", "display_order": 4},
]


OPTIONAL_INSTRUMENTS = [
    {"code": "LNG", "name": "Liquefied Natural Gas (LNG)", "symbol": "LNG", "report_section": "COMMODITIES & ENERGY", "display_order": 2},
    {"code": "LNGJKM", "name": "Liquefied Natural Gas (LNG)", "symbol": "LNGJKM", "report_section": "COMMODITIES & ENERGY", "display_order": 2},
    {"code": "JKM", "name": "Liquefied Natural Gas (LNG)", "symbol": "JKM", "report_section": "COMMODITIES & ENERGY", "display_order": 2},
]

EXPECTED_BY_CODE = {item["code"]: item for item in EXPECTED_INSTRUMENTS + OPTIONAL_INSTRUMENTS}


NEWS_FEEDS = {
"global": [
    {
        "source": "BBC World",
        "url": "http://feeds.bbci.co.uk/news/world/rss.xml",
        "max": 15,
    },
    {
        "source": "BBC Europe",
        "url": "http://feeds.bbci.co.uk/news/world/europe/rss.xml",
        "max": 15,
    },
    {
        "source": "BBC US & Canada",
        "url": "http://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml",
        "max": 15,
    },
    {
        "source": "BBC Asia",
        "url": "http://feeds.bbci.co.uk/news/world/asia/rss.xml",
        "max": 15,
    },
    {
        "source": "Al Jazeera",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "max": 15,
    },
    {
        "source": "CNBC World",
        "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html",
        "max": 10,
    },
],
        # --- GCC business press ---
        {
            "source": "Khaleej Times",
            "url": "https://www.khaleejtimes.com/rss/business",
            "max": 10,
        },
        {
            "source": "Gulf News",
            "url": "https://gulfnews.com/business/rss",
            "max": 10,
        },
        {
            "source": "Arab News",
            "url": "https://www.arabnews.com/rss/business.xml",
            "max": 10,
        },
        # --- Google News query feeds (aggregate Reuters/Bloomberg/FT/WSJ for us) ---
        {
            "source": "US Markets",
            "url": "https://news.google.com/rss/search?q=US+markets+Fed+Wall+Street+Treasury&hl=en-US&gl=US&ceid=US:en",
            "max": 10,
        },
        {
            "source": "GCC Markets",
            "url": "https://news.google.com/rss/search?q=Saudi+UAE+GCC+oil+OPEC+aramco&hl=en&gl=US&ceid=US:en",
            "max": 10,
        },
        {
            "source": "Energy Markets",
            "url": "https://news.google.com/rss/search?q=oil+OPEC+Brent+LNG+gas+energy+markets&hl=en&gl=US&ceid=US:en",
            "max": 10,
        },
    ],
    "qatar": [
        {
            "source": "The Peninsula",
            "url": "https://thepeninsulaqatar.com/rss/category/Qatar-Business",
            "max": 10,
        },
        {
            "source": "Qatar Tribune",
            "url": "https://www.qatar-tribune.com/rss",
            "max": 10,
        },
        {
            "source": "Gulf Times",
            "url": "https://www.gulf-times.com/rss/business",
            "max": 10,
        },
    ],
}


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.replace(",", "").replace("%", "").strip()
            if cleaned.lower() in ("", "n/a", "na", "null", "none"):
                return None
            return float(cleaned)
        return float(value)
    except Exception:
        return None




def _is_valid_px_for_code(code: str, px: Optional[float]) -> bool:
    if px is None:
        return False
    rng = PRICE_RANGES.get(code)
    if not rng:
        return True
    return rng[0] <= float(px) <= rng[1]


def _pct_float(current: Optional[float], base: Optional[float]) -> Optional[float]:
    if current is None or base in (None, 0):
        return None
    try:
        return ((float(current) - float(base)) / float(base)) * 100.0
    except Exception:
        return None


def _format_pct_value(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):+.{digits}f}%"
    except Exception:
        return "N/A"


def _reasonable_1d_pct(code: str, pct: Optional[float]) -> bool:
    if pct is None:
        return False
    limit = MAX_REASONABLE_1D_PCT.get(code, MAX_REASONABLE_1D_PCT["default"])
    return abs(float(pct)) <= limit


def _to_int(value: Any, default: int = 999) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _parse_date(value: Any) -> Optional[datetime.date]:
    if value is None:
        return None
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    try:
        return datetime.datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _format_price(value: Optional[float], digits: int = 2):
    if value is None:
        return "N/A"
    try:
        rounded = round(float(value), digits)
        return rounded
    except Exception:
        return "N/A"


def _fmt_pct_from_value(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):+.{digits}f}%"
    except Exception:
        return "N/A"


def _fmt_pct_number(current: Optional[float], base: Optional[float], digits: int = 2) -> str:
    if current is None or base in (None, 0):
        return "N/A"
    try:
        pct = ((current - base) / base) * 100
        return f"{pct:+.{digits}f}%"
    except Exception:
        return "N/A"


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _supabase_headers() -> Dict[str, str]:
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_key:
        raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY environment variable")

    return {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
    }


def _supabase_base_url() -> str:
    supabase_url = os.environ.get("SUPABASE_URL")
    if not supabase_url:
        raise RuntimeError("Missing SUPABASE_URL environment variable")
    return supabase_url.rstrip("/")


def _supabase_get(path: str, params: Optional[Dict[str, str]] = None) -> Any:
    url = f"{_supabase_base_url()}/rest/v1/{path}"
    response = requests.get(
        url,
        headers=_supabase_headers(),
        params=params or {},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _get_latest_available_date(today: datetime.date) -> Optional[datetime.date]:
    params = {
        "select": "as_of_date",
        "order": "as_of_date.desc",
        "limit": "1",
    }

    rows = _supabase_get(SUPABASE_TABLE, params=params)

    if not rows:
        return None

    return _parse_date(rows[0].get("as_of_date"))


def _get_rows_for_date(as_of_date: datetime.date) -> List[Dict[str, Any]]:
    params = {
        "select": "*",
        "as_of_date": f"eq.{as_of_date.isoformat()}",
        "order": "report_section.asc,display_order.asc,instrument_code.asc",
    }

    rows = _supabase_get(SUPABASE_TABLE, params=params)
    return rows or []


def _get_history_rows_for_calculations(as_of_date: datetime.date) -> List[Dict[str, Any]]:
    year_start = datetime.date(as_of_date.year, 1, 1)
    history_start = year_start - datetime.timedelta(days=10)

    params = {
        "select": "instrument_code,px_last,change_1d_pct,as_of_date,status,source",
        "as_of_date": f"gte.{history_start.isoformat()}",
        "order": "instrument_code.asc,as_of_date.asc",
    }

    rows = _supabase_get(SUPABASE_TABLE, params=params)
    return rows or []


def _group_history_by_code(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for row in rows:
        code = row.get("instrument_code")
        if not code:
            continue
        grouped.setdefault(code, []).append(row)

    for code in grouped:
        grouped[code].sort(key=lambda r: str(r.get("as_of_date", "")))

    return grouped


DERIVED_QAR_CROSSES = {
    "CHFQAR": {
        "name": "CHF/QAR",
        "symbol": "CHFQAR",
        "quote_code": "USDCHF",
        "display_order": 4,
    },
    "CNYQAR": {
        "name": "CNY/QAR",
        "symbol": "CNYQAR",
        "quote_code": "USDCNY",
        "display_order": 5,
    },
}


def _derive_qar_cross_row(
    as_of_date: datetime.date,
    code: str,
    cfg: Dict[str, Any],
    usdqar_row: Dict[str, Any],
    quote_row: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    usdqar = _to_float(usdqar_row.get("px_last"))
    quote = _to_float(quote_row.get("px_last"))

    if usdqar is None or quote in (None, 0):
        return None

    px = usdqar / quote

    return {
        "as_of_date": as_of_date.isoformat(),
        "instrument_code": code,
        "instrument_name": cfg["name"],
        "symbol": cfg["symbol"],
        "yahoo_symbol": None,
        "report_section": "QAR CROSS RATES",
        "display_order": cfg["display_order"],
        "px_last": round(px, 6),
        "change_1d_pct": None,
        "source": "derived_from_usdqar_and_spot_fx_runtime",
        "source_url": "",
        "status": "valid_runtime_derived_fx",
    }


def _derive_missing_qar_cross_rows_for_date(
    rows: List[Dict[str, Any]],
    effective_date: datetime.date,
) -> List[Dict[str, Any]]:
    by_code = {str(row.get("instrument_code")): row for row in rows if row.get("instrument_code")}
    usdqar_row = by_code.get("USDQAR")

    if not usdqar_row:
        return rows

    out = list(rows)

    for code, cfg in DERIVED_QAR_CROSSES.items():
        if code in by_code:
            continue
        quote_row = by_code.get(cfg["quote_code"])
        if not quote_row:
            continue
        derived = _derive_qar_cross_row(effective_date, code, cfg, usdqar_row, quote_row)
        if derived:
            out.append(derived)

    return out


def _derive_missing_qar_cross_history_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_date: Dict[datetime.date, Dict[str, Dict[str, Any]]] = {}

    for row in rows:
        row_date = _parse_date(row.get("as_of_date"))
        code = row.get("instrument_code")
        if row_date is None or not code:
            continue
        by_date.setdefault(row_date, {})[str(code)] = row

    out = list(rows)

    for row_date, rows_by_code in by_date.items():
        usdqar_row = rows_by_code.get("USDQAR")
        if not usdqar_row:
            continue
        for code, cfg in DERIVED_QAR_CROSSES.items():
            if code in rows_by_code:
                continue
            quote_row = rows_by_code.get(cfg["quote_code"])
            if not quote_row:
                continue
            derived = _derive_qar_cross_row(row_date, code, cfg, usdqar_row, quote_row)
            if derived:
                out.append(derived)

    return out


def _row_is_usable_for_calculation(row: Dict[str, Any], code: str = "") -> bool:
    status = str(row.get("status") or "").lower()
    if status.startswith("invalid") or "outlier" in status or "quarantine" in status:
        return False
    px = _to_float(row.get("px_last"))
    return _is_valid_px_for_code(code, px)


def _last_px_before_or_on(
    history: List[Dict[str, Any]],
    target_date: datetime.date,
    code: str = "",
) -> Optional[float]:
    found = None

    for row in history:
        row_date = _parse_date(row.get("as_of_date"))
        if row_date is None:
            continue
        if row_date <= target_date and _row_is_usable_for_calculation(row, code):
            found = _to_float(row.get("px_last"))

    return found


def _previous_valid_row_before_date(
    history: List[Dict[str, Any]],
    target_date: datetime.date,
    code: str = "",
) -> Optional[Dict[str, Any]]:
    found = None

    for row in history:
        row_date = _parse_date(row.get("as_of_date"))
        if row_date is None or row_date >= target_date:
            continue
        if _row_is_usable_for_calculation(row, code):
            found = row

    return found


def _digits_for_code(code: str) -> int:
    if code in {
        "USDQAR",
        "EURQAR",
        "GBPQAR",
        "CHFQAR",
        "CNYQAR",
        "EURUSD",
        "GBPUSD",
        "USDCHF",
        "USDCNY",
        "USDJPY",
    }:
        return 4

    if code in {"DHBK", "CBQK", "MARK", "DUBK", "ABQK", "QIIB", "LESHA"}:
        return 3

    if code in {"UST5Y", "UST10Y"}:
        return 4

    if code in {"BRENT", "SILVER", "GOLD", "LNGJK", "LNG", "LNGJKM", "JKM"}:
        return 2

    return 2

def _normalise_market_row(
    row: Dict[str, Any],
    history_by_code: Dict[str, List[Dict[str, Any]]],
    effective_date: datetime.date,
) -> Dict[str, Any]:
    code = row.get("instrument_code") or ""
    expected = EXPECTED_BY_CODE.get(code, {})

    px_last = _to_float(row.get("px_last"))
    history = history_by_code.get(code, [])
    prev_row = _previous_valid_row_before_date(history, effective_date, code)
    prev_px = _to_float(prev_row.get("px_last")) if prev_row else None
    prev_date = _parse_date(prev_row.get("as_of_date")) if prev_row else None

    month_start = datetime.date(effective_date.year, effective_date.month, 1)
    year_start = datetime.date(effective_date.year, 1, 1)

    month_base = _last_px_before_or_on(history, month_start - datetime.timedelta(days=1), code)
    year_base = _last_px_before_or_on(history, year_start - datetime.timedelta(days=1), code)

    # 1D logic:
    # Calculate 1D only from Supabase price history.
    # Do not use Make/Yahoo supplied change_1d_pct, because Make is now price-only.
    # If no valid recent prior market row exists, show N/A rather than a false +0.00%.
    calc_1d_pct = None
    if prev_date is not None and 0 < (effective_date - prev_date).days <= 4:
        calc_1d_pct = _pct_float(px_last, prev_px)

    if _reasonable_1d_pct(code, calc_1d_pct):
        change_1d = _format_pct_value(calc_1d_pct, 2)
    else:
        change_1d = "N/A"

    mtd = _fmt_pct_number(px_last, month_base, 2)
    ytd = _fmt_pct_number(px_last, year_base, 2)

    report_section = row.get("report_section") or expected.get("report_section") or "UNKNOWN"

    return {
        "code": code,
        "name": row.get("instrument_name") or expected.get("name") or code,
        "ticker": row.get("symbol") or expected.get("symbol") or code,
        "px_last": _format_price(px_last, _digits_for_code(code)),
        "change_1d": change_1d,
        "mtd": mtd,
        "ytd": ytd,
        "as_of": str(row.get("as_of_date") or effective_date.isoformat()),
        "source": row.get("source") or "Supabase",
        "status": row.get("status") or "valid",
        "report_section": report_section,
        "display_order": _to_int(row.get("display_order"), expected.get("display_order", 999)),
    }


def fetch_market_data_from_supabase(today: datetime.date) -> tuple[Dict[str, List[Dict[str, Any]]], List[str], Optional[datetime.date]]:
    issues: List[str] = []

    latest_date = _get_latest_available_date(today)
    if latest_date is None:
        raise RuntimeError(f"No rows found in Supabase table {SUPABASE_TABLE}")

    effective_date = latest_date

    if effective_date != today:
        delta_days = (today - effective_date).days
        issues.append(f"Using latest available Supabase market date {effective_date.isoformat()}, not today {today.isoformat()}")

        if delta_days > STALE_DATA_WARNING_DAYS:
            issues.append(f"Supabase market data is stale by {delta_days} days")

    rows = _get_rows_for_date(effective_date)
    rows = _derive_missing_qar_cross_rows_for_date(rows, effective_date)

    history_rows = _get_history_rows_for_calculations(effective_date)
    history_rows = _derive_missing_qar_cross_history_rows(history_rows)
    history_by_code = _group_history_by_code(history_rows)

    expected_codes = {item["code"] for item in EXPECTED_INSTRUMENTS}
    optional_codes = {item["code"] for item in OPTIONAL_INSTRUMENTS}
    known_codes = expected_codes | optional_codes
    actual_codes = {row.get("instrument_code") for row in rows if row.get("instrument_code")}
    mandatory_actual_codes = actual_codes & expected_codes

    missing_codes = sorted(expected_codes - actual_codes)
    extra_codes = sorted(actual_codes - known_codes)

    if missing_codes:
        issues.append(f"Missing instruments from Supabase: {', '.join(missing_codes)}")

    if extra_codes:
        issues.append(f"Unexpected instruments in Supabase: {', '.join(extra_codes)}")

    if len(mandatory_actual_codes) != EXPECTED_INSTRUMENT_COUNT:
        issues.append(f"Expected {EXPECTED_INSTRUMENT_COUNT} mandatory instruments, found {len(mandatory_actual_codes)}")

    output = {
        "global_indices": [],
        "gcc_indices": [],
        "spot_currency": [],
        "qar_cross_rates": [],
        "fixed_income": [],
        "qatari_banks": [],
        "commodities": [],
    }

    normalised_rows = [
        _normalise_market_row(row, history_by_code, effective_date)
        for row in rows
        if row.get("instrument_code") in known_codes
    ]

    for row in normalised_rows:
        section_name = row.get("report_section", "")
        output_key = REPORT_SECTION_TO_OUTPUT_KEY.get(section_name)

        if not output_key:
            issues.append(f"Unknown report section for {row.get('code')}: {section_name}")
            continue

        clean_row = {
            "code": row["code"],
            "name": row["name"],
            "ticker": row["ticker"],
            "px_last": row["px_last"],
            "change_1d": row["change_1d"],
            "mtd": row["mtd"],
            "ytd": row["ytd"],
            "as_of": row["as_of"],
            "source": row["source"],
            "status": row["status"],
        }

        output[output_key].append((row["display_order"], clean_row))

    for key in output:
        output[key] = [
            row for _, row in sorted(output[key], key=lambda item: item[0])
        ]

    return output, issues, effective_date


def _find_row(rows: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    for row in rows:
        if row.get("name") == name:
            return row
    return None


def validate_market_data(data: Dict[str, Any]) -> List[str]:
    issues: List[str] = []

    for issue in data.get("_supabase_issues", []):
        issues.append(issue)

    total_market_rows = sum(
        len(data.get(section, []))
        for section in [
            "global_indices",
            "gcc_indices",
            "spot_currency",
            "qar_cross_rates",
            "fixed_income",
            "qatari_banks",
            "commodities",
        ]
    )

    if total_market_rows != EXPECTED_INSTRUMENT_COUNT:
        issues.append(f"Market row count mismatch: expected {EXPECTED_INSTRUMENT_COUNT}, found {total_market_rows}")

    qe = _find_row(data.get("gcc_indices", []), "Qatar QE Index")
    doha = _find_row(data.get("qatari_banks", []), "Doha")
    usdqar = _find_row(data.get("qar_cross_rates", []), "USD/QAR")
    spx = _find_row(data.get("global_indices", []), "US S&P 500")
    nifty = _find_row(data.get("global_indices", []), "India Nifty 50")
    fadgi = _find_row(data.get("gcc_indices", []), "Abu Dhabi ADX")
    oman = _find_row(data.get("gcc_indices", []), "Oman")
    gbpusd = _find_row(data.get("spot_currency", []), "GBP/USD")
    usdchf = _find_row(data.get("spot_currency", []), "USD/CHF")
    usdjpy = _find_row(data.get("spot_currency", []), "USD/JPY")
    bka = _find_row(data.get("gcc_indices", []), "Kuwait Boursa")
    gold = _find_row(data.get("commodities", []), "Gold")
    lng = _find_row(data.get("commodities", []), "LNG JP/KR")
    chfqar = _find_row(data.get("qar_cross_rates", []), "CHF/QAR")
    cnyqar = _find_row(data.get("qar_cross_rates", []), "CNY/QAR")
    lesha = _find_row(data.get("qatari_banks", []), "Lesha")

    required_rows = [
        ("Qatar QE Index", qe),
        ("Doha Bank price", doha),
        ("US S&P 500", spx),
        ("India Nifty 50", nifty),
        ("Abu Dhabi ADX", fadgi),
        ("Oman", oman),
        ("GBP/USD", gbpusd),
        ("USD/CHF", usdchf),
        ("USD/JPY", usdjpy),
        ("Kuwait Boursa", bka),
        ("CHF/QAR", chfqar),
        ("CNY/QAR", cnyqar),
        ("Lesha", lesha),
        ("LNG JP/KR", lng),
        ("Gold", gold),
    ]

    for label, row in required_rows:
        if not row or row.get("px_last") in (None, "N/A", ""):
            issues.append(f"{label} missing or invalid")

    def numeric_px(row: Optional[Dict[str, Any]]) -> Optional[float]:
        if not row:
            return None
        return _to_float(row.get("px_last"))

    if numeric_px(usdjpy) is not None and numeric_px(usdjpy) < 100:
        issues.append(f"USD/JPY suspicious value: {usdjpy.get('px_last')}")

    if numeric_px(gbpusd) is not None and numeric_px(gbpusd) < 1:
        issues.append(f"GBP/USD suspicious value: {gbpusd.get('px_last')}")

    if numeric_px(usdchf) is not None and not (0.5 <= numeric_px(usdchf) <= 1.2):
        issues.append(f"USD/CHF suspicious value: {usdchf.get('px_last')}")

    if numeric_px(bka) is not None and numeric_px(bka) < 1000:
        issues.append(f"Kuwait Boursa suspicious value: {bka.get('px_last')}")

    if numeric_px(gold) is not None and not (1000 <= numeric_px(gold) <= 7000):
        issues.append(f"Gold suspicious value: {gold.get('px_last')}")

    if usdqar and usdqar.get("change_1d") not in (None, "N/A"):
        try:
            raw = str(usdqar["change_1d"]).replace("%", "").strip()
            val = float(raw)
            if abs(val) > USD_QAR_SUSPICIOUS_MOVE_THRESHOLD:
                issues.append(f"USD/QAR daily change suspicious: {usdqar['change_1d']}")
        except Exception:
            issues.append("USD/QAR daily change unparsable")

    return issues


def _parse_news_datetime(value: Any) -> Optional[datetime.datetime]:
    if not value:
        return None
    try:
        if isinstance(value, datetime.datetime):
            dt = value
        else:
            dt = parsedate_to_datetime(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def _is_relevant_global_item(item: Dict[str, Any], now_utc: datetime.datetime) -> bool:
    """
    Light filter for the global news pool. An item is kept if:
      - it mentions at least one US/GCC/energy/geopolitics keyword, AND
      - it does NOT match any noise keyword (sports, entertainment, etc), AND
      - it is recent enough (within GLOBAL_NEWS_MAX_AGE_HOURS) when a date is parseable.
    Final story selection is done by Claude in summarise_news().
    """
    title = _clean_text(item.get("title") or item.get("headline") or "")
    summary = _clean_text(item.get("summary") or item.get("description") or "")
    blob = f"{title} {summary}".lower()

    if any(bad in blob for bad in GLOBAL_EXCLUDE_KEYWORDS):
        return False

    if not any(word in blob for word in GLOBAL_FOCUS_KEYWORDS):
        return False

    dt = _parse_news_datetime(item.get("published"))
    if dt is not None:
        age_hours = (now_utc - dt).total_seconds() / 3600
        if age_hours < -2 or age_hours > GLOBAL_NEWS_MAX_AGE_HOURS:
            return False

    return True


def _brave_global_news() -> List[Dict[str, Any]]:
    api_key = os.environ.get("BRAVE_API_KEY")
    if not api_key:
        print("[WARN] BRAVE_API_KEY not set, global Brave fallback skipped.")
        return []

    out: List[Dict[str, Any]] = []
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
    }

    for query in GLOBAL_NEWS_BRAVE_QUERIES:
        try:
            response = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers=headers,
                params={
                    "q": query,
                    "count": "10",
                    "search_lang": "en",
                    "freshness": "pd",
                    "safesearch": "moderate",
                },
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            for result in payload.get("web", {}).get("results", []) or []:
                title = _clean_text(result.get("title", ""))
                summary = _clean_text(result.get("description", ""))
                url = result.get("url", "") or ""
                source = result.get("profile", {}).get("name") or "Brave Search"
                published = result.get("age") or result.get("page_age") or ""
                if not title:
                    continue
                out.append({
                    "source": source,
                    "title": title,
                    "summary": summary[:500],
                    "link": url,
                    "published": published,
                })
        except Exception as exc:
            print(f"[WARN] Brave global news query failed: {query} | {exc}")

    return out


def fetch_global_news() -> List[Dict[str, Any]]:
    """
    Fetch from the global feed list, apply US/GCC relevance filter,
    and top up via Brave Search if we don't have enough survivors.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    raw = dedupe_news(fetch_news(NEWS_FEEDS["global"]))
    filtered = [item for item in raw if _is_relevant_global_item(item, now_utc)]

    # If feeds gave us thin coverage, top up from Brave
    if len(filtered) < GLOBAL_NEWS_TARGET_COUNT * 2:
        brave_items = dedupe_news(_brave_global_news())
        # Brave items don't always have a parseable date — apply only keyword filter
        for item in brave_items:
            blob = f"{item.get('title','')} {item.get('summary','')}".lower()
            if any(bad in blob for bad in GLOBAL_EXCLUDE_KEYWORDS):
                continue
            if not any(word in blob for word in GLOBAL_FOCUS_KEYWORDS):
                continue
            filtered.append(item)
        filtered = dedupe_news(filtered)

    print(f"    Global news after US/GCC relevance filter: {len(filtered)}")
    return filtered


def _is_recent_qatar_business_item(item: Dict[str, Any], now_utc: datetime.datetime) -> bool:
    title = _clean_text(item.get("title") or item.get("headline") or "")
    summary = _clean_text(item.get("summary") or item.get("description") or "")
    source = _clean_text(item.get("source") or "")
    blob = f"{title} {summary} {source}".lower()

    if "qatar" not in blob and "doha" not in blob and "qnb" not in blob and "qse" not in blob:
        return False

    if any(bad in blob for bad in QATAR_EXCLUDE_KEYWORDS):
        return False

    if not any(word in blob for word in QATAR_BUSINESS_KEYWORDS):
        return False

    dt = _parse_news_datetime(item.get("published"))
    if dt is not None:
        age_hours = (now_utc - dt).total_seconds() / 3600
        if age_hours < -2 or age_hours > QATAR_NEWS_MAX_AGE_HOURS:
            return False

    return True




def _extract_qatar_page_items() -> List[Dict[str, Any]]:
    """Best-effort scraper for user-approved Qatar business pages.
    It does not invent news. It extracts titles/links from the business pages,
    then the recency filter is applied downstream where dates are available.
    """
    items: List[Dict[str, Any]] = []
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DohaBankMarketIntel/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    link_re = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.I | re.S)
    date_re = re.compile(r'(\d{1,2}\s+[A-Z][a-z]{2}\s+2026|\d{1,2}/\d{1,2}/2026|2026-\d{2}-\d{2})')

    for page_url in QATAR_BUSINESS_PAGES:
        try:
            resp = requests.get(page_url, headers=headers, timeout=30)
            if resp.status_code >= 400:
                print(f"[WARN] Qatar page fetch failed {page_url}: HTTP {resp.status_code}")
                continue
            html = resp.text
            source = "Qatar Tribune" if "qatar-tribune" in page_url else "The Peninsula Qatar"
            for href, anchor_html in link_re.findall(html):
                title = _clean_text(anchor_html)
                if len(title) < 12:
                    continue
                if title.lower() in {"business", "read more", "home", "next", "previous"}:
                    continue
                if href.startswith("/"):
                    base = "https://www.qatar-tribune.com" if "qatar-tribune" in page_url else "https://thepeninsulaqatar.com"
                    href = base + href
                if not href.startswith("http"):
                    continue
                m = date_re.search(html[max(0, html.find(anchor_html) - 250): html.find(anchor_html) + 500] if anchor_html in html else "")
                published = m.group(1) if m else ""
                items.append({
                    "source": source,
                    "title": title,
                    "summary": title,
                    "link": href,
                    "published": published,
                })
        except Exception as exc:
            print(f"[WARN] Qatar business page scrape failed {page_url}: {exc}")
    return items


def _brave_qatar_news() -> List[Dict[str, Any]]:
    api_key = os.environ.get("BRAVE_API_KEY")
    if not api_key:
        print("[WARN] BRAVE_API_KEY not set, Qatar Brave fallback skipped.")
        return []

    out: List[Dict[str, Any]] = []
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
    }

    for query in QATAR_NEWS_BRAVE_QUERIES:
        try:
            response = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers=headers,
                params={
                    "q": query,
                    "count": "10",
                    "country": "qa",
                    "search_lang": "en",
                    "freshness": "pd",
                    "safesearch": "moderate",
                },
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            for result in payload.get("web", {}).get("results", []) or []:
                title = _clean_text(result.get("title", ""))
                summary = _clean_text(result.get("description", ""))
                url = result.get("url", "") or ""
                source = result.get("profile", {}).get("name") or "Brave Search"
                published = result.get("age") or result.get("page_age") or ""
                if not title:
                    continue
                out.append({
                    "source": source,
                    "title": title,
                    "summary": summary[:500],
                    "link": url,
                    "published": published,
                })
        except Exception as exc:
            print(f"[WARN] Brave Qatar news query failed: {query} | {exc}")

    return out


def fetch_qatar_business_news() -> List[Dict[str, Any]]:
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    raw = dedupe_news(fetch_news(NEWS_FEEDS["qatar"]) + _extract_qatar_page_items())
    filtered = [item for item in raw if _is_recent_qatar_business_item(item, now_utc)]

    if len(filtered) < QATAR_NEWS_MIN_VALID_COUNT:
        brave_items = dedupe_news(_brave_qatar_news())
        combined = dedupe_news(filtered + brave_items)
        filtered = [item for item in combined if _is_recent_qatar_business_item(item, now_utc)]

    print(f"    Qatar valid recent business news items: {len(filtered)}")
    return filtered[:QATAR_NEWS_TARGET_COUNT]

def fetch_news(feed_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []

    for feed_cfg in feed_list:
        try:
            feed = feedparser.parse(feed_cfg["url"])
            print(f"    RSS {feed_cfg['source']} entries: {len(feed.entries)}")

            for entry in feed.entries[: feed_cfg["max"]]:
                title = _clean_text(entry.get("title", ""))
                summary = _clean_text(getattr(entry, "summary", ""))
                link = entry.get("link", "")
                published = entry.get("published", "") or entry.get("updated", "")

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


def dedupe_news(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []

    for item in items:
        key = re.sub(r"[^a-z0-9]+", "", item.get("title", "").lower())[:120]

        if not key or key in seen:
            continue

        seen.add(key)
        out.append(item)

    return out


def ensure_min_news(items: List[Dict[str, Any]], count: int, fallback_source: str) -> List[Dict[str, Any]]:
    out = list(items)

    placeholders = [
        {
            "source": fallback_source,
            "title": "Market news source temporarily unavailable",
            "summary": "The approved source feed returned no usable article in this cycle.",
            "link": "",
            "published": "",
        },
        {
            "source": fallback_source,
            "title": "Business news feed refresh pending",
            "summary": "The workflow will retry the same approved source on the next run.",
            "link": "",
            "published": "",
        },
        {
            "source": fallback_source,
            "title": "Market coverage awaiting source update",
            "summary": "Approved publisher feed did not return enough items for this report cycle.",
            "link": "",
            "published": "",
        },
        {
            "source": fallback_source,
            "title": "Economy headline stream incomplete",
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


def _fallback_summarise_news(raw_items: List[Dict[str, Any]], count: int) -> List[Dict[str, Any]]:
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


def summarise_news(raw_items: List[Dict[str, Any]], scope: str, count: int) -> List[Dict[str, Any]]:
    if not raw_items:
        return []

    api_key = os.environ.get("ANTHROPIC_API_KEY")

    if not api_key:
        print("[WARN] ANTHROPIC_API_KEY not set, using fallback summarisation.")
        return _fallback_summarise_news(raw_items, count)

    client = anthropic.Anthropic(api_key=api_key)

    headlines_txt = "\n".join(
        f"[{item['source']}] {item['title']} — {item['summary']} (URL: {item['link']})"
        for item in raw_items
    )

    system = (
        "You are a financial news editor for a Gulf bank daily market intelligence report. "
        "Return only valid JSON. Select the most relevant stories and produce clean metric boxes."
    )

    priority_hint = ""
    if "us" in scope.lower() or "gcc" in scope.lower() or "global" in scope.lower():
        priority_hint = (
            "\nPRIORITISE stories in this order:\n"
            "  1. US Federal Reserve / Treasury yields / inflation / Wall Street\n"
            "  2. GCC region: Saudi Arabia, UAE, Kuwait, Bahrain, Oman (markets, sovereign funds, energy)\n"
            "  3. OPEC+ decisions, oil price moves, LNG / gas\n"
            "  4. Geopolitics that moves Gulf markets (Iran, Israel, Hormuz, sanctions)\n"
            "  5. Major US/EU corporate or banking stories with global market impact\n"
            "Avoid: sports, entertainment, celebrity, lifestyle.\n"
        )

    prompt = f"""
From the following {scope} news items, select the {count} most relevant stories.
{priority_hint}
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
            model=os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
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


def build_kpis(market_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    def find_by_code(rows: List[Dict[str, Any]], code: str):
        code = str(code or "").upper()
        for row in rows:
            if str(row.get("code") or "").upper() == code:
                return row
        return None

    def value(row: Optional[Dict[str, Any]], field: str, default: str = "N/A"):
        if not row:
            return default
        v = row.get(field)
        return v if v not in (None, "", "N/A") else default

    def format_number(v):
        if isinstance(v, (int, float)):
            return f"{v:,.2f}"
        return str(v or "N/A")

    commodities = market_data.get("commodities", [])
    gcc_indices = market_data.get("gcc_indices", [])
    qatari_banks = market_data.get("qatari_banks", [])

    lng_row = find_by_code(commodities, "LNGJK")
    gold_row = find_by_code(commodities, "GOLD")
    qse_row = find_by_code(gcc_indices, "QE")
    doha_row = find_by_code(qatari_banks, "DHBK")

    return [
        {
            "value": format_number(value(lng_row, "px_last")),
            "label": "LNG JP/KR",
            "sublabel": f"{value(lng_row, 'change_1d')} today · USD/MMBtu",
        },
        {
            "value": format_number(value(gold_row, "px_last")),
            "label": "Gold",
            "sublabel": f"{value(gold_row, 'change_1d')} today · {value(gold_row, 'ytd')} YTD",
        },
        {
            "value": format_number(value(qse_row, "px_last")),
            "label": "QSE Index",
            "sublabel": f"{value(qse_row, 'change_1d')} today · {value(qse_row, 'ytd')} YTD",
        },
        {
            "value": format_number(value(doha_row, "px_last")),
            "label": "Doha Bank PX Last",
            "sublabel": f"{value(doha_row, 'change_1d')} today · {value(doha_row, 'ytd')} YTD",
        },
    ]


def run() -> Dict[str, Any]:
    today = datetime.date.today()
    cfg = CONFIG

    generated_at_utc = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    data: Dict[str, Any] = {
        "config": cfg,
        "generated_at": generated_at_utc,
        "generated_display_time": cfg.get("delivery_time_ast", "07:00") + " AST",
    }

    print("▶ Fetching market data from Supabase ...")

    try:
        market_sections, supabase_issues, effective_date = fetch_market_data_from_supabase(today)

        for key, rows in market_sections.items():
            if not cfg["sections"].get(key, True):
                data[key] = []
            else:
                data[key] = rows

            print(f"  · {key}: {len(data[key])} rows")

        data["_supabase_issues"] = supabase_issues
        data["market_as_of_date"] = effective_date.isoformat() if effective_date else None

    except Exception as e:
        print(f"[ERROR] Supabase market fetch failed: {e}")

        for key in [
            "global_indices",
            "gcc_indices",
            "spot_currency",
            "qar_cross_rates",
            "fixed_income",
            "qatari_banks",
            "commodities",
        ]:
            data[key] = []

        data["_supabase_issues"] = [f"Supabase market fetch failed: {e}"]
        data["market_as_of_date"] = None

    if cfg["sections"].get("global_news", True):
        print("  · global news (US + GCC focus)")
        raw_global = fetch_global_news()
        raw_global = ensure_min_news(raw_global, GLOBAL_NEWS_TARGET_COUNT, "Reuters/Bloomberg")
        data["global_news"] = summarise_news(
            raw_global, "US politics, Europe, China, geopolitics, GCC, technology, AI, energy and major world developments", GLOBAL_NEWS_TARGET_COUNT
        )
    else:
        data["global_news"] = []

    qatar_valid_count = 0
    if cfg["sections"].get("qatar_news", True):
        print("  · qatar news")
        raw_qatar = fetch_qatar_business_news()
        qatar_valid_count = len(raw_qatar)
        if raw_qatar:
            data["qatar_news"] = summarise_news(raw_qatar, "qatar", min(QATAR_NEWS_TARGET_COUNT, len(raw_qatar)))
        else:
            data["qatar_news"] = []
    else:
        data["qatar_news"] = []

    data["_qatar_valid_news_count"] = qatar_valid_count

    data["kpis"] = build_kpis(data)

    validation_issues = validate_market_data(data)
    if cfg["sections"].get("qatar_news", True) and data.get("_qatar_valid_news_count", 0) < QATAR_NEWS_MIN_VALID_COUNT:
        validation_issues.append(
            f"CRITICAL: Qatar news has only {data.get('_qatar_valid_news_count', 0)} valid recent business news items, minimum required {QATAR_NEWS_MIN_VALID_COUNT}"
        )
    data["validation_issues"] = validation_issues
    data["report_status"] = "ok" if not validation_issues else "needs_review"

    if "_supabase_issues" in data:
        del data["_supabase_issues"]
    if "_qatar_valid_news_count" in data:
        del data["_qatar_valid_news_count"]

    print("✓ Fetch complete.")

    if validation_issues:
        print("⚠ Validation issues found:")
        for issue in validation_issues:
            print(f"   - {issue}")
    else:
        print("✓ Validation passed.")

    return data


if __name__ == "__main__":
    result = run()

    with open("market_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)

    print("✓ Data written to market_data.json")

    

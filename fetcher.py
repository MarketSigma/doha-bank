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
        "market_drivers": True,
    }
}


SUPABASE_TABLE = "market_indices_history"
EXPECTED_INSTRUMENT_COUNT = 39
STALE_DATA_WARNING_DAYS = 3
USD_QAR_SUSPICIOUS_MOVE_THRESHOLD = 10.0

QATAR_NEWS_TARGET_COUNT = 6
QATAR_NEWS_MIN_VALID_COUNT = 3
QATAR_NEWS_MAX_AGE_HOURS = 24

GLOBAL_NEWS_TARGET_COUNT = 8
GLOBAL_NEWS_MIN_FLOOR = 4       # hard minimum — past-week top-up fires if pass-1 yields fewer
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
    "kuwait", "bahrain", "oman",
    # NOTE: 'qatar' deliberately excluded from this list. Qatar-focused
    # stories belong in the Qatar section, not the global section.
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
    # --- US macro & monetary policy ---
    'Federal Reserve Powell interest rates today',
    'US Treasury yields bond market today',
    'US inflation CPI PCE economy today',
    'US jobs report nonfarm payrolls unemployment',
    # --- US equities & corporate ---
    'Wall Street S&P 500 Nasdaq Dow Jones today',
    'US corporate earnings results tech banks',
    # --- FX / dollar ---
    'US dollar DXY currency markets today',
    # --- GCC (non-Qatar) ---
    'Saudi Arabia Aramco PIF Vision 2030 economy',
    'UAE Dubai Abu Dhabi ADNOC Mubadala ADIA business',
    'Kuwait Bahrain Oman GCC business economy markets',
    # --- Energy ---
    'OPEC oil production Brent crude price today',
    'LNG natural gas market price Europe Asia',
    # --- Geopolitics moving Gulf markets ---
    'Middle East Iran Israel Hormuz markets oil',
    # --- Major non-US developed markets ---
    'ECB Bank of England Bank of Japan central bank',
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


# ============================================================
# Source-quality controls
# ============================================================
# Brave Search returns the open web, including SEO farms, low-quality
# aggregators and rumour blogs. We accept Brave items only when their host
# is on this allowlist. RSS feeds are not filtered here because each feed
# is already a curated publisher.
# ============================================================

CREDIBLE_GLOBAL_DOMAINS = {
    # ---- US / global wires & agencies ----
    "reuters.com", "bloomberg.com", "apnews.com", "afp.com",
    # ---- Top financial press ----
    "ft.com", "wsj.com", "nytimes.com", "washingtonpost.com",
    "economist.com", "cnbc.com", "marketwatch.com", "barrons.com",
    # ---- Asian financial press ----
    "nikkei.com", "asia.nikkei.com", "scmp.com", "straitstimes.com",
    "japantimes.co.jp",
    # ---- US TV networks ----
    "cnn.com", "nbcnews.com", "abcnews.go.com", "cbsnews.com",
    # ---- UK / European wire-grade ----
    "bbc.com", "bbc.co.uk", "theguardian.com",
    # ---- US policy / national press ----
    "politico.com", "npr.org", "axios.com",
    # ---- GCC / MENA quality press (Qatar EXCLUDED — those go to Qatar section) ----
    "arabnews.com", "alarabiya.net", "aljazeera.com",
    "thenationalnews.com", "thenational.ae",
    "khaleejtimes.com", "gulfnews.com", "gulfbusiness.com",
    "arabianbusiness.com", "saudigazette.com.sa",
    "english.aawsat.com",
    "agbi.com",
    # ---- Central banks & multilateral institutions ----
    "federalreserve.gov", "treasury.gov", "sec.gov",
    "ecb.europa.eu", "bankofengland.co.uk", "boj.or.jp",
    "bis.org", "imf.org", "worldbank.org",
    "opec.org", "iea.org",
}

# Premium / wire-grade subset of the global allowlist. Items from these
# domains are sorted to the top of the candidate list so they reach Claude
# first and get the prime story slots. With the wire-grade-only trim, this
# now overlaps almost entirely with the credible set.
PREMIUM_GLOBAL_DOMAINS = {
    # Wires
    "reuters.com", "bloomberg.com", "apnews.com", "afp.com",
    # Top financial press
    "ft.com", "wsj.com", "nytimes.com", "washingtonpost.com",
    "economist.com", "cnbc.com", "marketwatch.com", "barrons.com",
    # Asian financial press
    "nikkei.com", "asia.nikkei.com", "scmp.com",
    # UK
    "bbc.com", "bbc.co.uk", "theguardian.com",
    # Top GCC
    "arabnews.com", "thenationalnews.com", "thenational.ae",
    "agbi.com", "alarabiya.net", "aljazeera.com",
    # Central banks / officials
    "federalreserve.gov", "treasury.gov", "sec.gov",
    "ecb.europa.eu", "bankofengland.co.uk", "boj.or.jp",
    "bis.org", "imf.org", "worldbank.org",
    "opec.org", "iea.org",
}

CREDIBLE_QATAR_DOMAINS = {
    # Qatar press
    "thepeninsulaqatar.com", "peninsulaqatar.com",
    "qatar-tribune.com", "gulf-times.com",
    # Qatar official
    "qna.org.qa", "qatarenergy.qa", "qcb.gov.qa",
    # Reputable international coverage of Qatar
    "reuters.com", "bloomberg.com", "ft.com", "wsj.com",
    "aljazeera.com", "arabnews.com", "thenationalnews.com",
    "khaleejtimes.com", "gulfnews.com", "agbi.com",
    "gulfbusiness.com", "arabianbusiness.com",
}

# Clean display labels for known publishers. The key is the registrable
# domain, the value is the human-readable name shown in the report.
DOMAIN_DISPLAY_NAMES = {
    "reuters.com": "Reuters",
    "bloomberg.com": "Bloomberg",
    "apnews.com": "AP News",
    "afp.com": "AFP",
    "ft.com": "Financial Times",
    "wsj.com": "Wall Street Journal",
    "nytimes.com": "New York Times",
    "washingtonpost.com": "Washington Post",
    "cnbc.com": "CNBC",
    "marketwatch.com": "MarketWatch",
    "barrons.com": "Barron's",
    "economist.com": "The Economist",
    "theguardian.com": "The Guardian",
    "bbc.com": "BBC",
    "bbc.co.uk": "BBC",
    "cnn.com": "CNN",
    "nbcnews.com": "NBC News",
    "abcnews.go.com": "ABC News",
    "cbsnews.com": "CBS News",
    "nikkei.com": "Nikkei",
    "asia.nikkei.com": "Nikkei Asia",
    "scmp.com": "South China Morning Post",
    "straitstimes.com": "The Straits Times",
    "japantimes.co.jp": "Japan Times",
    "politico.com": "Politico",
    "npr.org": "NPR",
    "axios.com": "Axios",
    "arabnews.com": "Arab News",
    "saudigazette.com.sa": "Saudi Gazette",
    "alarabiya.net": "Al Arabiya",
    "thenationalnews.com": "The National",
    "thenational.ae": "The National",
    "khaleejtimes.com": "Khaleej Times",
    "gulfnews.com": "Gulf News",
    "gulfbusiness.com": "Gulf Business",
    "arabianbusiness.com": "Arabian Business",
    "english.aawsat.com": "Asharq Al-Awsat",
    "agbi.com": "AGBI",
    "aljazeera.com": "Al Jazeera",
    "thepeninsulaqatar.com": "The Peninsula",
    "peninsulaqatar.com": "The Peninsula",
    "qatar-tribune.com": "Qatar Tribune",
    "gulf-times.com": "Gulf Times",
    "qna.org.qa": "QNA",
    "qatarenergy.qa": "QatarEnergy",
    "qcb.gov.qa": "QCB",
    "federalreserve.gov": "Federal Reserve",
    "treasury.gov": "US Treasury",
    "sec.gov": "SEC",
    "ecb.europa.eu": "ECB",
    "bankofengland.co.uk": "Bank of England",
    "boj.or.jp": "Bank of Japan",
    "bis.org": "BIS",
    "imf.org": "IMF",
    "worldbank.org": "World Bank",
    "opec.org": "OPEC",
    "iea.org": "IEA",
}

# Terms that mark a story as Qatar-focused. If any appear in the headline,
# the item is routed to the Qatar section only and excluded from global.
QATAR_FOCUS_TERMS = (
    "qatar", "doha", "qse", "qnb", "qatarenergy", "qib", "qcb",
    "qatari", "al udeid",
)




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
    "BKA": (7000, 12000),
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
    {"code": "BKA", "name": "Kuwait", "symbol": "^BKP.KW", "report_section": "GCC & REGIONAL INDICES", "display_order": 5},
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
            "max": 6,
        },
        {
            "source": "Al Jazeera",
            "url": "https://www.aljazeera.com/xml/rss/all.xml",
            "max": 6,
        },

        # --- GCC business press ---
        {
            "source": "Khaleej Times",
            "url": "https://www.khaleejtimes.com/rss/business",
            "max": 4,
        },
        {
            "source": "Gulf News",
            "url": "https://gulfnews.com/business/rss",
            "max": 4,
        },

        # --- Google News query feeds ---
        {
            "source": "US Markets",
            "url": "https://news.google.com/rss/search?q=US+markets+Fed+Wall+Street+Treasury&hl=en-US&gl=US&ceid=US:en",
            "max": 4,
        },
        {
            "source": "GCC Markets",
            "url": "https://news.google.com/rss/search?q=Saudi+UAE+GCC+oil+OPEC+aramco&hl=en&gl=US&ceid=US:en",
            "max": 4,
        },
        {
            "source": "Energy Markets",
            "url": "https://news.google.com/rss/search?q=oil+OPEC+Brent+LNG+gas+energy+markets&hl=en&gl=US&ceid=US:en",
            "max": 4,
        },
    ],

    "qatar": [
        {
            "source": "The Peninsula",
            "url": "https://thepeninsulaqatar.com/rss/category/Qatar-Business",
            "max": 4,
        },
        {
            "source": "Qatar Tribune",
            "url": "https://www.qatar-tribune.com/rss",
            "max": 4,
        },
        {
            "source": "Gulf Times",
            "url": "https://www.gulf-times.com/rss/business",
            "max": 4,
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


def _domain_of(url: str) -> str:
    """Return the registrable host of a URL (lowercase, no www/m/amp prefix)."""
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower()
        host = re.sub(r"^www\.", "", host)
        host = re.sub(r"^(m|amp|edition|news)\.", "", host)
        return host
    except Exception:
        return ""


def _host_matches_allowlist(host: str, allowlist) -> bool:
    """True if host equals or is a subdomain of any entry in the allowlist."""
    if not host:
        return False
    for allowed in allowlist:
        if host == allowed or host.endswith("." + allowed):
            return True
    return False


def _is_credible_global_source(url: str) -> bool:
    return _host_matches_allowlist(_domain_of(url), CREDIBLE_GLOBAL_DOMAINS)


def _is_credible_qatar_source(url: str) -> bool:
    return _host_matches_allowlist(_domain_of(url), CREDIBLE_QATAR_DOMAINS)


def _is_qatar_focused_item(item: Dict[str, Any]) -> bool:
    """True if the headline strongly implies the story is about Qatar.
    Used to keep Qatar content out of the global news section."""
    title = (item.get("title") or "").lower()
    return any(term in title for term in QATAR_FOCUS_TERMS)


def _source_from_url(url: str) -> str:
    """Derive a clean, human-readable source label from a URL.

    Uses DOMAIN_DISPLAY_NAMES for known publishers (e.g. 'reuters.com' ->
    'Reuters'). Falls back to a title-cased version of the host's
    registrable label for unknown domains.
    """
    host = _domain_of(url)
    if not host:
        return ""

    # Exact match first
    if host in DOMAIN_DISPLAY_NAMES:
        return DOMAIN_DISPLAY_NAMES[host]

    # Subdomain match (e.g. uk.reuters.com -> reuters.com)
    for allowed_host, display in DOMAIN_DISPLAY_NAMES.items():
        if host.endswith("." + allowed_host):
            return display

    # Fallback: title-case the registrable label
    parts = host.split(".")
    label = parts[-2] if len(parts) >= 2 else host
    return label.replace("-", " ").title()


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
    """
    Get one usable row per expected instrument.

    Primary logic:
    - Prefer exact as_of_date.
    - If missing, carry forward the latest available row before as_of_date.
    - Do not carry forward rows older than 5 days.
    """

    expected_codes = [item["code"] for item in EXPECTED_INSTRUMENTS]
    rows_out = []

    for code in expected_codes:
        params = {
            "select": "*",
            "instrument_code": f"eq.{code}",
            "as_of_date": f"lte.{as_of_date.isoformat()}",
            "order": "as_of_date.desc",
            "limit": "1",
        }

        rows = _supabase_get(SUPABASE_TABLE, params=params) or []

        if rows:
            rows_out.append(rows[0])

    rows_out.sort(
        key=lambda r: (
            str(r.get("report_section") or ""),
            _to_int(r.get("display_order")),
            str(r.get("instrument_code") or ""),
        )
    )

    return rows_out


def _get_history_rows_for_calculations(as_of_date: datetime.date) -> List[Dict[str, Any]]:
    year_start = datetime.date(as_of_date.year, 1, 1)
    history_start = year_start - datetime.timedelta(days=10)

    all_rows = []
    batch_size = 1000
    offset = 0

    while True:
        params = {
            "select": "instrument_code,px_last,change_1d_pct,as_of_date,status,source",
            "as_of_date": f"gte.{history_start.isoformat()}",
            "order": "instrument_code.asc,as_of_date.asc",
            "limit": str(batch_size),
            "offset": str(offset),
        }

        rows = _supabase_get(SUPABASE_TABLE, params=params) or []
        all_rows.extend(rows)

        if len(rows) < batch_size:
            break

        offset += batch_size

    print(f"  · history rows loaded for calculations: {len(all_rows)}")

    return all_rows


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
    carried_forward = []

    for row in rows:
        row_date = _parse_date(row.get("as_of_date"))
        code = row.get("instrument_code")

        if row_date and row_date < effective_date:
            days_old = (effective_date - row_date).days

            carried_forward.append(
                f"{code} carried forward from {row_date.isoformat()} ({days_old} day(s) old)"
            )

            row["status"] = f"carry_forward_previous_available_{days_old}d"
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
    if carried_forward:
        issues.extend(carried_forward)
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
      - the headline is NOT Qatar-focused (Qatar has its own section), AND
      - it is recent enough (within GLOBAL_NEWS_MAX_AGE_HOURS) when a date is parseable.
    Final story selection is done by Claude in summarise_news().
    """
    # Qatar-focused stories belong in the Qatar section, never in global.
    if _is_qatar_focused_item(item):
        return False

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


def _brave_global_news(freshness: str = "pd") -> List[Dict[str, Any]]:
    """Run all global Brave queries with the given freshness window.

    freshness: 'pd' = past day, 'pw' = past week, 'pm' = past month.
    """
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
                    "freshness": freshness,
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
                profile_name = (result.get("profile") or {}).get("name") or ""
                # Prefer the URL-derived source so attribution is accurate and
                # generic strings like "Brave Search" never leak to the model.
                source = _source_from_url(url) or profile_name or "Web"
                published = result.get("age") or result.get("page_age") or ""
                if not title or not url:
                    continue
                out.append({
                    "source": source,
                    "title": title,
                    "summary": summary[:500],
                    "link": url,
                    "published": published,
                })
        except Exception as exc:
            print(f"[WARN] Brave global news query failed ({freshness}): {query} | {exc}")

    return out


def _is_premium_global_source(url: str) -> bool:
    return _host_matches_allowlist(_domain_of(url), PREMIUM_GLOBAL_DOMAINS)


def _apply_global_brave_filter(brave_items, current_filtered):
    """Filter a batch of Brave items down to credible non-Qatar items.
    Returns (kept_list, drop_uncredible_count, drop_qatar_count, drop_excluded_count)."""
    kept = []
    drop_uncredible = drop_qatar = drop_excluded = 0
    for item in brave_items:
        blob = f"{item.get('title','')} {item.get('summary','')}".lower()
        if any(bad in blob for bad in GLOBAL_EXCLUDE_KEYWORDS):
            drop_excluded += 1
            continue
        if _is_qatar_focused_item(item):
            drop_qatar += 1
            continue
        if not _is_credible_global_source(item.get("link", "")):
            drop_uncredible += 1
            continue
        kept.append(item)
    return kept, drop_uncredible, drop_qatar, drop_excluded


def fetch_global_news() -> List[Dict[str, Any]]:
    """
    Fetch global news in up to two passes:
      1. RSS feeds + Brave Search (past day) over the credible-publisher allowlist.
      2. Brave Search (past week) — fires ONLY if pass 1 yielded fewer than
         GLOBAL_NEWS_MIN_FLOOR credible items. This guarantees the report
         meets the minimum-card rule (8 cards) even on quiet news days,
         while preferring same-day stories when they're available.

    Items are sorted so premium wires (Reuters, Bloomberg, FT, WSJ etc.)
    reach the summariser first.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    raw = dedupe_news(fetch_news(NEWS_FEEDS["global"]))
    print(f"    [global] RSS raw items (deduped): {len(raw)}")

    filtered = [item for item in raw if _is_relevant_global_item(item, now_utc)]
    print(f"    [global] After include/exclude/age filter on RSS: {len(filtered)}")

    if not os.environ.get("BRAVE_API_KEY"):
        print("    [global] BRAVE_API_KEY not set — Brave fallback skipped.")
    else:
        # --- Pass 1: past day ---
        brave_pd = dedupe_news(_brave_global_news(freshness="pd"))
        print(f"    [global] Brave (past day) raw items: {len(brave_pd)}")
        kept, du, dq, de = _apply_global_brave_filter(brave_pd, filtered)
        filtered.extend(kept)
        print(f"    [global] Brave (past day) kept: {len(kept)} "
              f"(dropped {du} non-credible, {dq} Qatar-focused, {de} excluded-noise)")

        filtered = dedupe_news(filtered)
        print(f"    [global] After RSS + Brave(pd) merge & dedupe: {len(filtered)}")

        # --- Pass 2: past week, only if we're below the minimum floor ---
        if len(filtered) < GLOBAL_NEWS_MIN_FLOOR:
            print(f"    [global] Only {len(filtered)} credible items so far — "
                  f"below the {GLOBAL_NEWS_MIN_FLOOR}-card minimum. Running "
                  f"Brave past-week top-up to meet the floor.")
            brave_pw = dedupe_news(_brave_global_news(freshness="pw"))
            print(f"    [global] Brave (past week) raw items: {len(brave_pw)}")
            kept2, du2, dq2, de2 = _apply_global_brave_filter(brave_pw, filtered)
            filtered.extend(kept2)
            print(f"    [global] Brave (past week) kept: {len(kept2)} "
                  f"(dropped {du2} non-credible, {dq2} Qatar-focused, {de2} excluded-noise)")
            filtered = dedupe_news(filtered)
            print(f"    [global] After past-week top-up & dedupe: {len(filtered)}")

    # Sort so premium wires reach Claude first; Claude picks the top items
    # and our prompt enforces US > GCC ordering.
    def _rank(item):
        url = item.get("link", "")
        # 0 = premium, 1 = credible-but-not-premium, 2 = anything else (RSS feeds)
        if _is_premium_global_source(url):
            return 0
        if _is_credible_global_source(url):
            return 1
        return 2

    filtered.sort(key=_rank)

    print(f"    [global] Final items to summariser: {len(filtered)}  "
          f"(premium: {sum(1 for i in filtered if _is_premium_global_source(i.get('link','')))})")
    return filtered


def _is_recent_qatar_business_item(item: Dict[str, Any], now_utc: datetime.datetime) -> bool:
    title = _clean_text(item.get("title") or item.get("headline") or "").lower()
    summary = _clean_text(item.get("summary") or item.get("description") or "").lower()
    source = _clean_text(item.get("source") or "").lower()

    # Qatar anchor — require Qatar reference in the actual content
    # (title or summary). The publisher name is deliberately EXCLUDED
    # from this check, so a Qatar Tribune article about Saudi politics
    # does not qualify as Qatar news. Content must be about Qatar.
    content_blob = f"{title} {summary}"
    qatar_terms = ("qatar", "doha", "qnb", "qse", "qib", "qcb", "qatarenergy", "qia")
    if not any(t in content_blob for t in qatar_terms):
        return False

    # The other relevance checks can still see the publisher, since
    # source-name false positives are not a concern there.
    blob = f"{content_blob} {source}"

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
                profile_name = (result.get("profile") or {}).get("name") or ""
                source = _source_from_url(url) or profile_name or "Web"
                published = result.get("age") or result.get("page_age") or ""
                if not title or not url:
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
    print(f"    [qatar] RSS + scrape raw items (deduped): {len(raw)}")

    filtered = [item for item in raw if _is_recent_qatar_business_item(item, now_utc)]
    print(f"    [qatar] After Qatar relevance + age filter on RSS: {len(filtered)}")

    # Top up from Brave if RSS coverage is thin. Trust the Brave query
    # (which already mentions 'Qatar') — only enforce the exclude list and
    # require 'qatar'/'doha'/'qnb'/'qse' to appear somewhere in the blob.
    if len(filtered) < QATAR_NEWS_MIN_VALID_COUNT:
        if not os.environ.get("BRAVE_API_KEY"):
            print("    [qatar] BRAVE_API_KEY not set — Brave fallback skipped.")
            brave_items: List[Dict[str, Any]] = []
        else:
            brave_items = dedupe_news(_brave_qatar_news())
            print(f"    [qatar] Brave raw items (deduped across queries): {len(brave_items)}")

        kept_from_brave = 0
        dropped_uncredible = 0
        for item in brave_items:
            title_l = (item.get("title") or "").lower()
            summary_l = (item.get("summary") or "").lower()
            source_l = (item.get("source") or "").lower()
            content_blob = f"{title_l} {summary_l}"
            full_blob = f"{content_blob} {source_l}"
            if any(bad in full_blob for bad in QATAR_EXCLUDE_KEYWORDS):
                continue
            # Qatar anchor — require the actual content (title or summary)
            # to be about Qatar. The publisher name is excluded from this
            # check: a Qatar Tribune article about Saudi affairs is not
            # Qatar news.
            if not any(k in content_blob for k in ("qatar", "doha", "qnb", "qse", "qib", "qcb", "qatarenergy")):
                continue
            # Only accept Qatar-press and reputable international coverage.
            if not _is_credible_qatar_source(item.get("link", "")):
                dropped_uncredible += 1
                continue
            filtered.append(item)
            kept_from_brave += 1
        print(f"    [qatar] Brave kept: {kept_from_brave} "
              f"(dropped {dropped_uncredible} non-credible)")

        filtered = dedupe_news(filtered)
        print(f"    [qatar] After merging RSS + Brave and deduping: {len(filtered)}")

    print(f"    [qatar] Final items to summariser: {len(filtered)}")
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

    # No placeholder padding — return only real items, even if fewer than count.
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
        "Return only valid JSON. Select the most relevant stories and produce clean metric boxes. "
        "CRITICAL: Every headline, summary, source and URL you return MUST be derived strictly "
        "from the news items provided. You must NEVER invent, fabricate, embellish, or guess "
        "any fact, number, name, quote, source attribution, or URL that is not present in the "
        "input. If an input item has a thin or empty body, keep your output equally thin — do "
        "not pad it with plausible-sounding details. If fewer than the requested count of real "
        "items are usable, return fewer items rather than fabricating to hit the target."
    )

    priority_hint = ""
    if "us" in scope.lower() or "gcc" in scope.lower() or "global" in scope.lower():
        priority_hint = (
            "\nORDERING is strict — output items in this priority order:\n"
            "  1. US world and business: Federal Reserve, Treasury yields, inflation, "
            "Wall Street (S&P, Nasdaq, Dow), US corporate earnings, US tariffs/policy, "
            "US dollar, major US political developments with market impact.\n"
            "  2. GCC world and business (EXCLUDING Qatar): Saudi Arabia, UAE, Kuwait, "
            "Bahrain, Oman — markets, sovereign wealth funds (PIF, ADIA, Mubadala, KIA), "
            "energy (Aramco, ADNOC), and major corporate or policy stories.\n"
            "  3. OPEC+ decisions, Brent / WTI / crude price moves, LNG and gas.\n"
            "  4. Geopolitics affecting Gulf markets (Iran, Israel, Hormuz, sanctions).\n"
            "  5. Major EU / UK / Asia stories with global market impact.\n"
            "ABSOLUTE RULES:\n"
            "  - DO NOT include any Qatar-focused story in this list — Qatar has its "
            "own dedicated section. If an item's primary subject is Qatar, Doha, QNB, "
            "QSE, QIA or QatarEnergy, SKIP it entirely.\n"
            "  - DO NOT include sports, entertainment, celebrity, lifestyle, weather, "
            "or traffic items.\n"
        )

    prompt = f"""
From the following {scope} news items, select AT MOST {count} of the most relevant stories.
{priority_hint}
Return a JSON array of UP TO {count} objects (fewer is acceptable if fewer real items qualify).

Each object must contain exactly these keys:
- headline
- summary
- source
- url
- metric
- metric_label

Strict rules:
- Use ONLY the source string exactly as it appears in brackets for that item. Do not
  rename, generalise, or re-attribute (e.g. do not relabel a "BBC World" item as
  "Reuters" or "Bloomberg").
- Use ONLY the URL exactly as provided in the item (after "URL:"). If empty, return "".
- The headline and summary must be supported by the title/summary of the SAME item.
  Do not introduce facts, figures, quotes, or names that are not in the input.
- headline maximum 10 words
- summary maximum 40 words
- metric must be meaningful, short, and never just a dash unless absolutely impossible
- metric_label must explain the metric briefly
- If an input item is clearly a placeholder, empty, or non-substantive, SKIP it
  rather than inventing content to fill the slot.
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
            # Defensive validation: only keep items whose source is one that
            # actually appeared in our input list. This stops the model from
            # silently relabelling something as "Reuters" or "Bloomberg".
            allowed_sources = {(it.get("source") or "").strip() for it in raw_items}
            allowed_sources.discard("")
            item_source = (item.get("source") or "").strip()
            if allowed_sources and item_source and item_source not in allowed_sources:
                # Drop the false attribution but keep the rest if URL is present in input
                input_urls = {(it.get("link") or "") for it in raw_items}
                if (item.get("url") or "") not in input_urls:
                    # Both source and URL are unfamiliar — likely fabricated, skip
                    continue
                # URL is real; coerce source to the matching input source
                for it in raw_items:
                    if (it.get("link") or "") == item.get("url"):
                        item_source = it.get("source") or item_source
                        break

            cleaned.append({
                "headline": (item.get("headline") or "")[:120] or "Market update",
                "summary": (item.get("summary") or "")[:240] or "Latest development relevant to markets.",
                "source": item_source or "Feed",
                "url": item.get("url", "") or "",
                "metric": (item.get("metric") or "")[:16] or "NEWS",
                "metric_label": (item.get("metric_label") or "")[:32] or "Signal",
            })

        # IMPORTANT: do NOT pad with placeholder cards. If Claude returned
        # fewer than `count` items because fewer real stories qualified,
        # return what we have. The PDF/report consumer is expected to
        # render only as many cards as there are entries.
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


# ============================================================
# Market Drivers — technical analysis section
# ------------------------------------------------------------
# Each driver is anchored to a real news item (so the source field
# carries through the publication name — Reuters, Bloomberg, FT,
# Gulf Times, etc.) but the headline and summary are TECHNICAL
# ANALYSIS for portfolio managers — specific levels to watch, the
# mechanism, what's at risk — not a news paraphrase.
#
# Output shape mirrors news items so the PDF/HTML generators can
# render them with the existing news-card vocabulary.
# ============================================================

MARKET_DRIVERS_TARGET_COUNT = 4
MARKET_DRIVERS_MIN_COUNT    = 3


def build_market_drivers(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Technical analysis section anchored to today's news.

    Each driver:
      - cites a real publication (source = e.g. "Reuters", "Bloomberg",
        "FT", "Gulf Times") taken from the news item it's anchored to
      - frames the implication as TECHNICAL ANALYSIS (levels, mechanism,
        what's at risk) — NOT a news summary

    Returns a list of dicts with the same shape as news items:
        { source, headline, summary, metric, metric_label }

    Returns [] on failure or when there's no news to anchor to.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[WARN] ANTHROPIC_API_KEY not set — skipping market drivers.")
        return []

    # --- Aggregate news as the candidate pool of trigger items ---
    # Each driver must anchor to one of these, so we have a real
    # publication name (and URL) to cite.
    all_news: List[Dict[str, str]] = []
    for item in data.get("global_news", []):
        all_news.append({
            "source":   (item.get("source") or "").strip(),
            "headline": (item.get("headline") or "").strip(),
            "summary":  (item.get("summary") or "").strip(),
            "url":      (item.get("url") or "").strip(),
        })
    for item in data.get("qatar_news", []):
        all_news.append({
            "source":   (item.get("source") or "").strip(),
            "headline": (item.get("headline") or "").strip(),
            "summary":  (item.get("summary") or "").strip(),
            "url":      (item.get("url") or "").strip(),
        })
    # Need a real source AND a real headline to anchor a driver
    all_news = [n for n in all_news if n["source"] and n["headline"]]

    if not all_news:
        print("  · [WARN] no news items available — skipping market drivers.")
        return []

    # --- Compact representation of biggest movers across sections ---
    def top_movers(rows: List[Dict[str, Any]], n: int = 3) -> List[Dict[str, Any]]:
        scored = []
        for r in rows:
            raw = str(r.get("change_1d") or "").replace("%", "").replace("+", "").strip()
            if not raw or raw.lower() in ("n/a", "na", "pegged", "none"):
                continue
            try:
                scored.append((abs(float(raw)), r))
            except (ValueError, TypeError):
                continue
        scored.sort(key=lambda x: -x[0])
        return [r for _, r in scored[:n]]

    movers_lines = []
    for section in ("global_indices", "gcc_indices", "commodities",
                    "spot_currency", "fixed_income"):
        for r in top_movers(data.get(section, []), n=3):
            movers_lines.append(
                f"  - {r.get('name', '')}: "
                f"px {r.get('px_last', 'N/A')}, "
                f"{r.get('change_1d', 'N/A')} 1D, "
                f"{r.get('mtd', 'N/A')} MTD, "
                f"{r.get('ytd', 'N/A')} YTD"
            )
    movers_block = "\n".join(movers_lines) if movers_lines else "  (no market data)"

    # --- News block with bracketed sources and URLs (matches summarise_news style) ---
    # Numbered so Claude can reference exactly which item it anchored to.
    news_lines = []
    for i, n in enumerate(all_news, 1):
        news_lines.append(
            f"  [{i}] [{n['source']}] {n['headline']} — {n['summary']} "
            f"(URL: {n.get('url', '')})"
        )
    news_block = "\n".join(news_lines)

    # Whitelists for defensive validation post-call
    allowed_sources = {n["source"] for n in all_news}
    allowed_urls    = {n["url"] for n in all_news if n["url"]}

    system = (
        "You are a senior markets strategist at Doha Bank writing the "
        "'Market Drivers' TECHNICAL ANALYSIS section for institutional "
        "clients (portfolio managers, treasury desks, sovereign wealth "
        "allocators). Each driver must cite a real publication (anchored "
        "to one of the news items provided) but your output is technical "
        "analysis — specific price levels, the transmission mechanism, "
        "what's at risk for portfolios — NOT a news summary. "
        "Tone: quantitative, forward-looking, specific. Use the levels, "
        "ranges and magnitudes from the inputs. Do not invent figures."
    )

    prompt = f"""
Today's biggest market movers:
{movers_block}

Today's news (numbered, with publication source in brackets):
{news_block}

Identify the {MARKET_DRIVERS_TARGET_COUNT} most important market drivers. Each driver MUST be
anchored to ONE of the numbered news items above. For each driver, output:

- source: the publication EXACTLY as it appears in brackets for the news
  item you anchored to (e.g. "Reuters", "Bloomberg", "FT", "Gulf Times",
  "QNA", "WSJ"). Do not invent or generalise the source.
- url: the URL EXACTLY as provided after "URL:" in the same news item
  you anchored the source to. Empty string if the news item has no URL.
- headline: max 14 words. TECHNICAL ANALYSIS framing — name the level
  to watch, the specific risk, the quantitative implication. NOT a
  news paraphrase.
- summary: max 50 words. The transmission mechanism + specific levels,
  ranges, dates, or magnitudes that matter. Written for portfolio
  managers, not retail.
- metric: short numeric or quantitative tag (e.g. "4.45%", "$82.34",
  "PMI 50.8", "1.0842", "48 mtpa")
- metric_label: 1-3 words explaining the metric (e.g. "UST10Y",
  "Brent spot", "EURUSD", "China Mfg PMI")

EXAMPLE — what GOOD looks like:
  Input news [3]: [Reuters] Fed minutes signal patience on rate cuts as inflation lingers
  GOOD driver output:
    {{
      "source": "Reuters",
      "url": "https://www.reuters.com/world/fed-minutes-...",
      "headline": "UST10Y in 4.40-4.60% range; break above 4.6% triggers rotation",
      "summary": "Fed patience anchors yields in current range. Sustained close above 4.60% on the 10y would pressure REITs, utilities and EM duration. Watch June FOMC dots for next directional signal.",
      "metric": "4.45%",
      "metric_label": "UST10Y"
    }}

  BAD driver output (rejected — this is news, not analysis):
    "headline": "Fed minutes signal patience on rate cuts"
    "summary": "Officials are divided over the timing of the first rate cut..."

Strict rules:
- Source MUST match a publication name from the bracketed news above.
  Do not output category tags like "Macro", "Energy", "FX" as the source.
- Headline and summary must be TECHNICAL ANALYSIS, not news rewrites.
- Cite specific numbers from the market data or news content; do not invent.
- Cover a mix of asset classes / themes — avoid {MARKET_DRIVERS_TARGET_COUNT} drivers about one topic.
- If fewer than {MARKET_DRIVERS_TARGET_COUNT} substantive drivers can be supported, return fewer.
- Return ONLY a JSON array. No markdown fences. No preamble.
"""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
            max_tokens=1800,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        parsed = json.loads(text)

        if not isinstance(parsed, list):
            raise ValueError("Claude did not return a list")

        cleaned = []
        for item in parsed[:MARKET_DRIVERS_TARGET_COUNT]:
            headline = (item.get("headline") or "").strip()
            summary  = (item.get("summary") or "").strip()
            source   = (item.get("source") or "").strip()

            if not headline or not summary:
                continue

            # Defensive: source must match an actual publication from the
            # news pool. Stops the model from regressing to category tags.
            if source not in allowed_sources:
                print(f"  · [WARN] dropping driver with unrecognised source "
                      f"'{source}' (not in news pool)")
                continue

            # Defensive: URL must match one we actually showed Claude.
            # If Claude returns a fabricated URL, blank it out — keep the
            # driver but make the headline non-clickable rather than route
            # the user to something we can't vouch for.
            url_raw = (item.get("url") or "").strip()
            url_clean = url_raw if url_raw in allowed_urls else ""

            cleaned.append({
                "source":       source[:24],
                "url":          url_clean,
                "headline":     headline[:140],
                "summary":      summary[:280],
                "metric":       (item.get("metric") or "")[:16],
                "metric_label": (item.get("metric_label") or "")[:32],
            })

        if len(cleaned) < MARKET_DRIVERS_MIN_COUNT:
            print(f"  · [WARN] market drivers returned only {len(cleaned)} items — "
                  f"below the {MARKET_DRIVERS_MIN_COUNT}-card minimum.")

        return cleaned

    except Exception as e:
        print(f"[WARN] Market drivers generation failed: {e}")
        return []


def run() -> Dict[str, Any]:
    today = datetime.date.today()
    cfg = CONFIG

    generated_at_utc = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Visibility into which credentials are reaching the script. If any of
    # these print False at runtime, fix the deployment env / secrets before
    # anything else — most "empty news" reports come from missing keys.
    print("▶ Environment check:")
    print(f"    ANTHROPIC_API_KEY set:        {bool(os.environ.get('ANTHROPIC_API_KEY'))}")
    print(f"    BRAVE_API_KEY set:            {bool(os.environ.get('BRAVE_API_KEY'))}")
    print(f"    SUPABASE_SERVICE_ROLE_KEY set:{bool(os.environ.get('SUPABASE_SERVICE_ROLE_KEY'))}")

    data: Dict[str, Any] = {
        "config": cfg,
        "generated_at": generated_at_utc,
        "generated_display_time": "",
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
        # NOTE: do NOT pre-pad raw_global with synthetic "Reuters/Bloomberg"
        # placeholders. Doing so caused the summariser to hallucinate plausible
        # Reuters/Bloomberg stories to fill the empty slots. We pass only real
        # items to Claude and pad the OUTPUT (not the input) with neutral,
        # unattributed slots inside summarise_news().
        data["global_news"] = summarise_news(
            raw_global,
            "US politics, Europe, China, geopolitics, GCC, technology, AI, energy and major world developments",
            GLOBAL_NEWS_TARGET_COUNT,
        )

        # Grid balance + floor rule:
        #   - 10+ items   -> take 10
        #   -  8 or 9     -> take 8 (drop the odd one out)
        #   - fewer than 8 is an unexpected edge case (would only happen if
        #     RSS + both Brave passes all failed). In that case fall through
        #     to whatever we have, but the past-week top-up should almost
        #     always prevent it.
        n = len(data["global_news"])
        if n >= 10:
            data["global_news"] = data["global_news"][:10]
        elif n >= 8:
            data["global_news"] = data["global_news"][:8]
        else:
            print(f"  · [WARN] global news only has {n} items — below the "
                  f"8-card minimum. Check Brave logs upstream.")
        print(f"  · global news after grid-balance + floor trim: "
              f"{len(data['global_news'])} cards")
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

    if cfg["sections"].get("market_drivers", True):
        print("  · market drivers (editorial synthesis)")
        data["market_drivers"] = build_market_drivers(data)
        print(f"  · market drivers: {len(data['market_drivers'])} items")
    else:
        data["market_drivers"] = []

    validation_issues = validate_market_data(data)
    if cfg["sections"].get("qatar_news", True) and data.get("_qatar_valid_news_count", 0) < QATAR_NEWS_MIN_VALID_COUNT:
        validation_issues.append(
            f"CRITICAL: Qatar news has only {data.get('_qatar_valid_news_count', 0)} valid recent business news items, minimum required {QATAR_NEWS_MIN_VALID_COUNT}"
        )
    data["validation_issues"] = validation_issues
    data["validation_issue_count"] = len(validation_issues)
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

    

    

    

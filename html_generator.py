"""
Doha Bank Market Updates - HTML Generator
=========================================

Companion to pdf_generator.py. Reads the same market_data.json and
produces a single self-contained, mobile-first .html file.

Design notes:
  - Same monochrome blue palette as the PDF
  - Same Caladea (serif) + Carlito (sans) typography, via Google Fonts
  - Same ▲▼● direction glyphs with the same colour conventions
  - Mobile-first: single column on phones; KPIs and news cards
    reflow to multi-column on tablet/desktop; tables horizontally
    scroll inside their container on narrow screens (the name
    column stays in view because it's the first cell of each row)
  - No JavaScript needed; all data is rendered at generation time
  - Adds the Market Drivers section after Qatar News
"""

import os
import sys
import json
import html
from datetime import date as dt


# ============================================================
# Formatting helpers (mirror the PDF generator's logic)
# ============================================================

def _to_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.replace(",", "").replace("%", "").strip()
            if cleaned.lower() in ("", "n/a", "na", "none", "null"):
                return None
            return float(cleaned)
        return float(value)
    except Exception:
        return None


def clean_px(px, code="", name=""):
    value = _to_float(px)
    if value is None:
        return "N/A"
    code = str(code or "").upper()
    if code in {"EURUSD", "GBPUSD", "USDCNY", "USDQAR", "EURQAR", "GBPQAR", "CNYQAR"}:
        return f"{value:,.4f}"
    if code == "USDJPY":
        return f"{value:,.2f}"
    if code in {"UST5Y", "UST10Y"}:
        return f"{value:,.4f}"
    if code in {"DHBK", "CBQK", "MARK", "DUBK", "ABQK", "QIIB"}:
        return f"{value:,.3f}"
    if code in {"QNBK", "QIBK"}:
        return f"{value:,.2f}"
    if abs(value) >= 1000:
        return f"{value:,.2f}"
    if abs(value) >= 10:
        return f"{value:,.2f}"
    if abs(value) < 1:
        return f"{value:,.4f}"
    return f"{value:,.2f}"


def safe(value, default="N/A"):
    if value is None:
        return default
    s = str(value)
    return s if s.strip() else default


def e(value):
    """Escape for HTML. Always pass user-supplied strings through this."""
    return html.escape(safe(value, default=""), quote=True)


def pct_cell(value):
    """
    Render a percentage as an HTML <td> with the right glyph and class.

    Conventions match the PDF:
      ▲ navy, bold      gains
      ▼ light blue      declines
      ● accent blue     neutral / pegged
      (none)            N/A or non-numeric
    """
    raw = str(value or "").strip()

    zero_tokens = {"Pegged", "0.00%", "0%", "+0.00%", "-0.00%", "+0%", "-0%"}
    if raw in zero_tokens:
        display = "Pegged" if raw == "Pegged" else "0.00%"
        return (
            '<td class="num pct pct-zero">'
            f'<span class="glyph">●</span><span class="value">{e(display)}</span>'
            '</td>'
        )
    if raw.startswith("+"):
        num = raw.lstrip("+")
        return (
            '<td class="num pct pct-up">'
            f'<span class="glyph">▲</span><span class="value">{e(num)}</span>'
            '</td>'
        )
    if raw.startswith("-"):
        num = raw.lstrip("-")
        return (
            '<td class="num pct pct-down">'
            f'<span class="glyph">▼</span><span class="value">{e(num)}</span>'
            '</td>'
        )
    if raw in ("", "N/A", "n/a", "—"):
        return '<td class="num pct pct-na"><span class="value">N/A</span></td>'
    return f'<td class="num pct"><span class="value">{e(raw)}</span></td>'


def kpi_change(sub):
    """KPI sublabel: same glyph treatment, returned as an inline-HTML fragment."""
    if not sub:
        return ""
    sub = str(sub)
    first = sub.split()[0] if sub else ""
    zero_tokens = {"Pegged", "0.00%", "0%", "+0.00%", "-0.00%", "+0%", "-0%"}

    if first in zero_tokens:
        rest = sub.split(" ", 1)[1] if " " in sub else ""
        display = "Pegged" if first == "Pegged" else "0.00%"
        body = f'{e(display)} {e(rest)}'.strip()
        return f'<span class="kpi-change pct-zero"><span class="glyph">●</span> {body}</span>'
    if sub.startswith("+"):
        return f'<span class="kpi-change pct-up"><span class="glyph">▲</span> {e(sub.lstrip("+"))}</span>'
    if sub.startswith("-"):
        return f'<span class="kpi-change pct-down"><span class="glyph">▼</span> {e(sub.lstrip("-"))}</span>'
    return f'<span class="kpi-change">{e(sub)}</span>'


# ============================================================
# Section builders
# ============================================================

def render_kpis(kpis):
    if not kpis:
        return ""
    cards = []
    for k in kpis:
        cards.append(
            '<div class="kpi-card">'
            f'<div class="kpi-label">{e(str(k.get("label", "")).upper())}</div>'
            f'<div class="kpi-value">{e(k.get("value", "—"))}</div>'
            f'<div class="kpi-sub">{kpi_change(k.get("sublabel", ""))}</div>'
            '</div>'
        )
    return f'<section class="kpi-strip">{"".join(cards)}</section>'


def render_table(title, meta, headers, rows):
    """Render a single data table card."""
    head_cells = "".join(
        f'<th class="{"col-name" if i == 0 else "num"}">{e(h.upper())}</th>'
        for i, h in enumerate(headers)
    )

    body_rows = []
    for r in rows:
        cells = []
        for ci, cell in enumerate(r):
            if ci == 0:
                cells.append(f'<td class="col-name">{e(cell)}</td>')
            elif ci == 1:
                cells.append(f'<td class="num px-last">{e(cell)}</td>')
            else:
                cells.append(pct_cell(cell))
        body_rows.append(f'<tr>{"".join(cells)}</tr>')

    meta_html = f'<span class="section-meta">{e(meta.upper())}</span>' if meta else ""

    return (
        '<section class="table-card">'
        f'<h2 class="section-header">{e(title.upper())}{meta_html}</h2>'
        '<div class="table-scroll">'
        '<table>'
        f'<thead><tr>{head_cells}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table>'
        '</div>'
        '</section>'
    )


def section_rows(data, sec):
    out = []
    for r in data.get(sec, []):
        out.append([
            r.get("name", ""),
            clean_px(r.get("px_last", "N/A"), code=r.get("code", ""), name=r.get("name", "")),
            r.get("change_1d", "N/A"),
            r.get("mtd", "N/A"),
            r.get("ytd", "N/A"),
        ])
    return out


def render_news_cards(title, meta, items):
    if not items:
        cards_html = '<p class="news-empty">No items.</p>'
    else:
        cards = []
        for item in items:
            headline_text = e(item.get("headline", ""))
            url = (item.get("url") or "").strip()
            if url and url.startswith(("http://", "https://")):
                headline_html = (
                    f'<a class="news-link" href="{e(url)}" '
                    f'target="_blank" rel="noopener noreferrer">{headline_text}</a>'
                )
                more_html = (
                    f'<a class="news-more" href="{e(url)}" '
                    f'target="_blank" rel="noopener noreferrer">Read more &rarr;</a>'
                )
            else:
                headline_html = headline_text
                more_html = ""

            cards.append(
                '<article class="news-card">'
                f'<div class="news-source">{e(str(item.get("source", "")).upper())}</div>'
                f'<h3 class="news-headline">{headline_html}</h3>'
                f'<p class="news-summary">{e(item.get("summary", ""))}</p>'
                f'{more_html}'
                '</article>'
            )
        cards_html = "".join(cards)

    meta_html = f'<span class="section-meta">{e(meta.upper())}</span>' if meta else ""

    return (
        '<section class="news-section">'
        f'<h2 class="section-header">{e(title.upper())}{meta_html}</h2>'
        f'<div class="news-grid">{cards_html}</div>'
        '</section>'
    )


# ============================================================
# CSS (mobile-first, with explicit breakpoints)
# ============================================================

CSS = r"""
:root {
  /* Doha Bank "A New Era" inspired palette */
  --brand-blue:   #062E63;
  --brand-deep:   #03244F;
  --brand-mid:    #1E5791;
  --gold:         #D4B58A;
  --gold-soft:    #E9D9C1;
  --silver:       #C8D4E2;
  --silver-light: #E9EEF4;

  --navy:        #082C5D;
  --mid-blue:    #45698F;
  --accent-blue: #7392B3;
  --light-blue:  #AFC1D4;
  --muted:       #7B8EA3;

  --white:   #FFFFFF;
  --tint:    #F7F9FC;
  --kpi-bg:  #FFFFFF;
  --border:  #E3E9F0;
  --page-bg: #F3F5F8;

  --serif: "Source Serif 4", "Cambria", "Georgia", serif;
  --sans:  "DM Sans", "Calibri", system-ui, -apple-system, "Segoe UI", sans-serif;

  --shadow-soft: 0 8px 24px rgba(3, 36, 79, 0.07);
  --shadow-card: 0 3px 12px rgba(3, 36, 79, 0.06);
}

* { box-sizing: border-box; }

html, body {
  margin: 0;
  padding: 0;
  background: var(--page-bg);
}

body {
  border-top: 5px solid var(--brand-blue); /* logo navy #062E63 */
  color: var(--navy);
  font-family: var(--sans);
  font-size: 16px;
  line-height: 1.48;
  -webkit-text-size-adjust: 100%;
}

/* ---------------- Masthead ----------------
   Logo + title adjacent (the mark stands alone, no wordmark),
   gold baseline rule below with edition label left / date right. */

.masthead {
  background: #FFFFFF;
  color: var(--navy);
  padding: 20px 18px 0;
}

.masthead-inner {
  max-width: 1200px;
  margin: 0 auto;
}

.brand-top {
  display: flex;
  align-items: center;
  gap: 22px;
}

.mast-logo {
  width: 68px;
  height: 95px;
  flex: 0 0 auto;
}

.masthead h1 {
  font-family: var(--serif);
  font-weight: 600;
  font-size: clamp(30px, 6.5vw, 52px);
  margin: 0;
  line-height: 1.02;
  letter-spacing: -0.02em;
  color: var(--brand-blue);
}

.baseline-rule {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  border-bottom: 2px solid var(--gold);
  margin-top: 12px;
  padding: 0 0 10px;
  font-size: 12px;
}

.edition-label {
  letter-spacing: 0.16em;
  font-weight: 700;
  color: var(--muted);
}

.report-date {
  font-weight: 600;
  color: #8A6F4A; /* deep gold */
  letter-spacing: 0.05em;
}

/* ---------------- Main container ---------------- */

main {
  max-width: 1200px;
  margin: 0 auto;
  padding: 14px 16px 18px;
}

/* ---------------- KPI strip ---------------- */

.kpi-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(215px, 1fr));
  gap: 10px;
  margin-bottom: 24px;
}

.kpi-card {
  position: relative;
  overflow: hidden;
  background: var(--kpi-bg);
  border: 1px solid var(--border);
  border-top: 3px solid var(--gold);
  padding: 13px 14px;
  border-radius: 8px;
  box-shadow: var(--shadow-card);
}

/* Branded bullet: a small solid Doha Bank crescent before each KPI label */
.kpi-label::before {
  content: "";
  display: inline-block;
  width: 9px;
  height: 13px;
  margin-right: 7px;
  vertical-align: -2px;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 60 84'%3E%3Cpath fill='%23062E63' d='M8 3 A39.7 39.7 0 1 1 14 81 A39.4 39.4 0 0 0 8 3 Z'/%3E%3Cpath fill='%23C2A57E' d='M10 14 A33 33 0 0 1 10 80 Z'/%3E%3C/svg%3E");
  background-size: contain;
  background-repeat: no-repeat;
}

.kpi-label {
  font-size: 10.5px;
  font-weight: 700;
  color: var(--mid-blue);
  letter-spacing: 0.14em;
  margin-bottom: 5px;
}

.kpi-value {
  font-family: var(--serif);
  font-weight: 600;
  font-size: clamp(23px, 5.4vw, 35px);
  color: var(--navy);
  letter-spacing: -0.02em;
  line-height: 1.08;
}

.kpi-sub {
  font-size: 12px;
  color: var(--mid-blue);
  margin-top: 5px;
}

.kpi-change .glyph { font-weight: 700; margin-right: 2px; }

/* ---------------- Section header ---------------- */

.section-header {
  font-size: 11px;
  font-weight: 700;
  color: var(--navy);
  letter-spacing: 0.19em;
  text-transform: uppercase;
  margin: 0 0 10px 0;
  padding-bottom: 7px;
  border-bottom: 2px solid var(--gold);
  display: flex;
  justify-content: space-between;
  align-items: baseline;
}

.section-meta {
  color: var(--accent-blue);
  font-weight: 600;
  font-size: 10px;
  letter-spacing: 0.14em;
}

/* ---------------- Tables ---------------- */

.tables-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 18px;
  margin-bottom: 28px;
}

@media (min-width: 820px) {
  .tables-grid { grid-template-columns: repeat(2, 1fr); gap: 22px; }
}

.table-card {
  background: var(--white);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px;
  box-shadow: var(--shadow-card);
}

.table-scroll {
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}

table {
  width: 100%;
  min-width: 380px;
  border-collapse: collapse;
  font-size: 13.5px;
}

thead th {
  font-size: 10px;
  font-weight: 700;
  color: var(--mid-blue);
  letter-spacing: 0.12em;
  text-align: right;
  padding: 8px 10px;
  border-bottom: 1px solid var(--silver);
  white-space: nowrap;
}

thead th.col-name { text-align: left; }

tbody td {
  padding: 9px 10px;
  border-bottom: 1px solid var(--border);
  vertical-align: middle;
  white-space: nowrap;
}

tbody tr:nth-child(even) { background: var(--tint); }
tbody tr:last-child td { border-bottom: none; }

td.col-name {
  font-weight: 600;
  color: var(--navy);
  text-align: left;
  white-space: normal;
  min-width: 130px;
}

td.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
  font-feature-settings: "tnum";
}

td.px-last { color: var(--navy); }

td.pct .value { font-variant-numeric: tabular-nums; }
td.pct .glyph { margin-right: 4px; font-weight: 700; }

td.pct-up { color: var(--navy); font-weight: 700; }
td.pct-up .glyph { color: var(--brand-mid); }

td.pct-down { color: var(--mid-blue); }
td.pct-down .glyph { color: var(--light-blue); }

td.pct-zero { color: var(--mid-blue); }
td.pct-zero .glyph { color: var(--gold); }

td.pct-na { color: var(--muted); }

/* ---------------- News sections ---------------- */

.news-section { margin-bottom: 28px; }

.news-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 12px;
}

@media (min-width: 700px) {
  .news-grid { grid-template-columns: repeat(2, 1fr); gap: 14px; }
}

.news-card {
  background: var(--white);
  border: 1px solid var(--border);
  border-left: 3px solid var(--gold);
  padding: 16px 18px;
  border-radius: 8px;
  box-shadow: var(--shadow-card);
}

.news-source {
  font-size: 11.5px;
  font-weight: 700;
  color: var(--accent-blue);
  letter-spacing: 0.14em;
  margin-bottom: 7px;
}

.news-headline {
  font-family: var(--serif);
  font-weight: 600;
  font-size: clamp(18px, 4.5vw, 25px);
  color: var(--navy);
  line-height: 1.27;
  margin: 0 0 9px 0;
}

.news-link {
  color: inherit;
  text-decoration: none;
  transition: color 0.15s ease, text-decoration-color 0.15s ease;
}

.news-link:hover,
.news-link:focus {
  color: var(--brand-mid);
  text-decoration: underline;
  text-decoration-thickness: 1.5px;
  text-underline-offset: 2px;
}

.news-link:focus { outline: 2px solid var(--gold); outline-offset: 2px; }

.news-summary {
  font-size: 15px;
  color: var(--mid-blue);
  line-height: 1.55;
  margin: 0;
}

.news-more {
  display: block;
  text-align: right;
  margin-top: 10px;
  font-family: var(--sans);
  font-size: 12.5px;
  font-weight: 700;
  color: var(--brand-mid);
  text-decoration: none;
  letter-spacing: 0.03em;
  transition: color 0.15s ease, text-decoration-color 0.15s ease;
}

.news-more:hover,
.news-more:focus {
  color: var(--brand-blue);
  text-decoration: underline;
  text-decoration-thickness: 1.5px;
  text-underline-offset: 2px;
}

.news-more:focus { outline: 2px solid var(--gold); outline-offset: 2px; }

.news-empty {
  color: var(--muted);
  font-size: 14px;
  font-style: italic;
}

/* ---------------- Footer ---------------- */

footer {
  position: relative;
  overflow: hidden;
  background: linear-gradient(135deg, var(--brand-deep), var(--brand-blue));
  color: var(--silver);
  padding: 14px 18px;
  margin-top: 24px;
  font-size: 11px;
  letter-spacing: 0.14em;
}

footer::after {
  content: "";
  position: absolute;
  width: 74px;
  height: 104px;
  right: 14px;
  top: 50%;
  transform: translateY(-50%);
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 60 84'%3E%3Cpath fill='%23FFFFFF' d='M8 3 A39.7 39.7 0 1 1 14 81 A39.4 39.4 0 0 0 8 3 Z'/%3E%3Cpath fill='%23C2A57E' d='M10 14 A33 33 0 0 1 10 80 Z'/%3E%3C/svg%3E");
  background-size: contain;
  background-repeat: no-repeat;
  opacity: 0.22;
}

footer .footer-inner {
  position: relative;
  z-index: 1;
  max-width: 1200px;
  margin: 0 auto;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
}

footer .brand-wordmark {
  color: var(--gold-soft);
  font-weight: 700;
  letter-spacing: 0.27em;
}

/* ---------------- Desktop bump ---------------- */

@media (min-width: 1024px) {
  body { font-size: 17px; }

  .brand-row { font-size: 12.5px; }
  .section-header { font-size: 12.5px; padding-bottom: 8px; }
  .section-meta { font-size: 11.5px; }

  .kpi-card { padding: 16px 18px; }
  .kpi-label { font-size: 12px; }
  .kpi-sub { font-size: 14px; margin-top: 6px; }

  table { font-size: 15.5px; }
  thead th { font-size: 11.5px; padding: 10px 12px; }
  tbody td { padding: 11px 12px; }

  .news-card { padding: 18px 22px; }
  .news-source { font-size: 13px; margin-bottom: 8px; }
  .news-summary { font-size: 17px; line-height: 1.6; }
  .news-more { font-size: 14px; margin-top: 12px; }
}

/* ---------------- Print ---------------- */

@media print {
  body { background: #fff; font-size: 11pt; }
  main { max-width: none; padding: 8mm; }
  .masthead, footer { padding: 8mm; }
  .news-card, .kpi-card, .table-card { break-inside: avoid; box-shadow: none; }
  footer::after { display: none; }
  .masthead::before { display: none; }
}
"""


# ============================================================
# Page assembly
# ============================================================

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#062E63">
<title>{title}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Source+Serif+4:opsz,wght@8..60,400;8..60,600;8..60,700&display=swap" rel="stylesheet">
<style>
{css}
</style>
</head>
<body>

<header class="masthead">
  <div class="masthead-inner">
    <div class="brand-top">
      <svg class="mast-logo" viewBox="0 0 60 84" aria-hidden="true" focusable="false">
        <path fill="#062E63" d="M8 3 A39.7 39.7 0 1 1 14 81 A39.4 39.4 0 0 0 8 3 Z"/>
        <path fill="#C2A57E" d="M10 14 A33 33 0 0 1 10 80 Z"/>
      </svg>
      <h1>{report_title}</h1>
    </div>
    <div class="baseline-rule">
      <span class="edition-label">{page_meta}</span>
      <span class="report-date">{report_date}</span>
    </div>
  </div>
</header>

<main>

{kpi_section}

<div class="tables-grid">
{table_html_left}
{table_html_right}
</div>

{news_global}
{news_qatar}
{news_drivers}

</main>

<footer>
  <div class="footer-inner">
    <span class="brand-wordmark">DOHA BANK</span>
    <span>{report_title_upper} &middot; {report_date}</span>
  </div>
</footer>

</body>
</html>
"""


def generate(data, output_path):
    cfg                    = data.get("config", {})
    report_date            = cfg.get("report_date", dt.today().strftime("%d %B %Y"))
    # Prepend weekday when the date parses cleanly: "Wednesday \u2022 22 July 2026"
    try:
        from datetime import datetime as _dtm
        _parsed = _dtm.strptime(report_date.strip(), "%d %B %Y")
        display_date = f"{_parsed.strftime('%A')} \u2022 {report_date.strip()}"
    except Exception:
        display_date = report_date
    report_title           = cfg.get("report_title", "Market Intelligence")
    generated_display_time = data.get("generated_display_time", "")

    page_meta = generated_display_time if generated_display_time else "DAILY MARKET BRIEF"

    kpi_html = render_kpis(data.get("kpis", []))

    # Left column tables
    table_html_left = (
        render_table("Global Indices",          "USD",       ["Market / Index", "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "global_indices")) +
        render_table("Spot Currency",           "FX",        ["Pair",            "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "spot_currency")) +
        render_table("QAR Cross Rates",         "QAR",       ["Pair",            "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "qar_cross_rates")) +
        render_table("Fixed Income · UST Yields", "YIELD %", ["Instrument",      "Yield",   "1D %", "MTD %", "YTD %"], section_rows(data, "fixed_income"))
    )

    # Right column tables
    table_html_right = (
        render_table("GCC & Regional Indices",  "LOCAL CCY", ["Market / Index", "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "gcc_indices")) +
        render_table("Qatari Banks",            "QAR",       ["Bank",           "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "qatari_banks")) +
        render_table("Commodities & Energy",    "USD",       ["Asset",          "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "commodities"))
    )

    news_global  = render_news_cards("Regional & Global News", None, data.get("global_news", [])[:5])
    news_qatar   = render_news_cards("Qatar News",             None, data.get("qatar_news",  [])[:4])
    news_drivers = render_news_cards("Market Drivers", "What Moved Markets", data.get("market_drivers", [])[:3])

    output = HTML_TEMPLATE.format(
        title=f"Doha Bank {e(report_title)} – {e(report_date)}",
        report_title=e(report_title),
        report_title_upper=e(report_title.upper()),
        css=CSS,
        page_meta=e(page_meta),
        report_date=e(display_date),
        kpi_section=kpi_html,
        table_html_left=table_html_left,
        table_html_right=table_html_right,
        news_global=news_global,
        news_qatar=news_qatar,
        news_drivers=news_drivers,
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output)

    size_bytes = os.path.getsize(output_path)
    print(f"HTML: {output_path}  |  {size_bytes / 1024:.0f} KB")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        data_path = sys.argv[1]
        out_path  = sys.argv[2]
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        generate(data, out_path)
    else:
        print("Usage: python html_generator.py market_data.json report.html")
        raise SystemExit(1)

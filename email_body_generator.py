"""
Doha Bank Market Updates - Email Body Renderer
==============================================

Produces an email-safe HTML string suitable for embedding directly in the
email body (not as an attachment). Same data and brand as html_generator.py
but compatible with the email client ecosystem (Gmail, Outlook, Apple Mail,
mobile clients).

Email-safe means:
  - Layout uses <table>, not CSS Grid / Flexbox
  - All styles are inline; no external CSS, no <style> blocks
  - No CSS variables, no @media queries, no clamp()
  - System fonts only (Georgia for serif, Arial/Helvetica for sans);
    no Google Fonts because most email clients strip @import
  - Hardcoded brand colours
  - Direction glyphs (▲▼●) are Unicode, rendered everywhere
  - All links absolute, opens in default browser
"""

import html
import os
from typing import Any, Dict, List

# Fallback logo source when config.logo_url is not supplied by the
# sender. Set LOGO_URL=cid:doha-logo (with a matching attachment) or
# to a hosted https:// URL.
DEFAULT_LOGO_URL = os.environ.get("LOGO_URL", "")


# ============================================================
# Brand palette — hardcoded; email clients ignore CSS variables
# ============================================================

BRAND_BLUE  = "#062E63"
BRAND_DEEP  = "#03244F"
GOLD        = "#D4B58A"
GOLD_LIGHT  = "#E4D3B4"
GOLD_DEEP   = "#8A6F4A"
SKY_BLUE    = "#D4B58A"
NAVY        = "#082C5D"
MID_BLUE    = "#45698F"
ACCENT_BLUE = "#7392B3"
LIGHT_BLUE  = "#AFC1D4"
SILVER      = "#C8D4E2"
MUTED       = "#7B8EA3"
WHITE       = "#FFFFFF"
TINT        = "#F7F9FC"
KPI_BG      = "#FFFFFF"
BORDER      = "#E3E9F0"
PAGE_BG     = "#F3F5F8"

# Cambria/Georgia stand in for Source Serif 4; Calibri/Segoe for DM Sans.
SERIF = "Cambria, Georgia, 'Times New Roman', serif"
SANS  = "Calibri, 'Segoe UI', Tahoma, Arial, sans-serif"
TITLE_FONT = SERIF


# ============================================================
# Formatting helpers — mirror the PDF/HTML generators
# ============================================================

def _e(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


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


def _clean_px(px, code: str = "") -> str:
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


def _pct_inline(value: Any) -> str:
    """Render a percentage value with directional glyph as an inline HTML span."""
    raw = str(value or "").strip()
    zero_tokens = {"Pegged", "0.00%", "0%", "+0.00%", "-0.00%", "+0%", "-0%"}
    if raw in zero_tokens:
        display = "Pegged" if raw == "Pegged" else "0.00%"
        return (
            f'<span style="color:{MID_BLUE};white-space:nowrap;">'
            f'<span style="color:{ACCENT_BLUE};font-weight:700;">●</span> {_e(display)}'
            '</span>'
        )
    if raw.startswith("+"):
        return (
            f'<span style="color:{NAVY};font-weight:700;white-space:nowrap;">'
            f'<span style="color:{NAVY};">▲</span> {_e(raw.lstrip("+"))}'
            '</span>'
        )
    if raw.startswith("-"):
        return (
            f'<span style="color:{MID_BLUE};white-space:nowrap;">'
            f'<span style="color:{LIGHT_BLUE};font-weight:700;">▼</span> {_e(raw.lstrip("-"))}'
            '</span>'
        )
    if raw in ("", "N/A", "n/a", "—"):
        return f'<span style="color:{MUTED};">N/A</span>'
    return f'<span style="color:{NAVY};">{_e(raw)}</span>'


def _kpi_change_inline(sub: str) -> str:
    """KPI sublabel: same glyph treatment, as inline HTML."""
    if not sub:
        return ""
    sub = str(sub)
    first = sub.split()[0] if sub else ""
    zero_tokens = {"Pegged", "0.00%", "0%", "+0.00%", "-0.00%", "+0%", "-0%"}

    if first in zero_tokens:
        rest = sub.split(" ", 1)[1] if " " in sub else ""
        display = "Pegged" if first == "Pegged" else "0.00%"
        body = f'{_e(display)} {_e(rest)}'.strip()
        return f'<span style="color:{ACCENT_BLUE};font-weight:700;">●</span> {body}'
    if sub.startswith("+"):
        return f'<span style="color:{NAVY};font-weight:700;">▲</span> {_e(sub.lstrip("+"))}'
    if sub.startswith("-"):
        return f'<span style="color:{LIGHT_BLUE};font-weight:700;">▼</span> {_e(sub.lstrip("-"))}'
    return _e(sub)


# ============================================================
# Section blocks
# ============================================================

def _masthead(report_date: str, generated_display_time: str,
              logo_url: str = "", report_title: str = "Market Intelligence") -> str:
    """Navy strip with the date, then the official Doha Bank lockup, the
    title, and the brief label above a gold rule — mirrors the browser HTML."""
    brief_label = generated_display_time or "DAILY MARKET BRIEF"

    # Email clients strip inline SVG and block data-URI images, so the lockup
    # needs a hosted (or CID) URL. Without one we fall back to the wordmark
    # set in type rather than showing a broken image.
    if logo_url:
        brand_block = (
            f'<img src="{_e(logo_url)}" width="204" height="70" alt="Doha Bank" '
            f'style="display:block;border:0;outline:none;text-decoration:none;'
            f'width:204px;height:70px;margin-bottom:14px;" />'
        )
    else:
        brand_block = (
            f'<div style="font-family:{SERIF};font-size:22px;font-weight:700;'
            f'letter-spacing:0.06em;color:{BRAND_BLUE};margin-bottom:12px;">'
            f'DOHA BANK</div>'
        )

    return f"""
<tr>
  <td class="eb-gutter" style="background:{BRAND_BLUE};padding:26px 22px;">
    <table cellpadding="0" cellspacing="0" border="0" width="100%">
      <tr>
        <td align="right" class="eb-date"
            style="font-family:{SANS};color:{GOLD_LIGHT};font-size:17px;
                   font-weight:700;letter-spacing:0.06em;line-height:1.2;">
          {_e(report_date)}
        </td>
      </tr>
    </table>
  </td>
</tr>

<tr>
  <td class="eb-gutter" style="background:{PAGE_BG};padding:22px 22px 0;">
    {brand_block}

    <div class="eb-title"
         style="font-family:{TITLE_FONT};color:{BRAND_BLUE};
                font-size:31px;font-weight:700;line-height:1.06;
                letter-spacing:-0.02em;margin:0;">
      {_e(report_title)}
    </div>

    <div class="eb-section-meta"
         style="font-family:{SANS};font-size:14px;font-weight:700;
                letter-spacing:0.16em;color:{MID_BLUE};margin-top:10px;">
      {_e(brief_label)}
    </div>

    <table cellpadding="0" cellspacing="0" border="0" width="100%"
           style="margin-top:18px;">
      <tr><td style="font-size:0;line-height:0;height:2px;
                     background:{GOLD};">&nbsp;</td></tr>
    </table>
  </td>
</tr>
"""


def _kpi_block(kpis: List[Dict[str, Any]]) -> str:
    if not kpis:
        return ""
    # 2 columns x N rows. Most days have 4 KPIs => 2x2.
    cells = []
    for k in kpis:
        label = str(k.get("label", "")).upper()
        value = str(k.get("value", "—"))
        sub   = _kpi_change_inline(str(k.get("sublabel", "")))
        cells.append(f'''
<td valign="top" style="padding:5px;" width="50%">
  <table cellpadding="0" cellspacing="0" border="0" width="100%"
         style="background:{KPI_BG};border:1px solid {BORDER};
                border-top:3px solid {GOLD};border-radius:8px;">
    <tr><td style="padding:13px 14px;">
      <div class="eb-kpi-label" style="font-family:{SANS};font-size:10.5px;font-weight:700;color:{MID_BLUE};letter-spacing:0.14em;">
        {_e(label)}
      </div>
      <div class="eb-kpi-value" style="font-family:{SERIF};font-size:28px;font-weight:700;color:{NAVY};margin-top:5px;line-height:1.08;letter-spacing:-0.02em;">
        {_e(value)}
      </div>
      <div class="eb-kpi-sub" style="font-family:{SANS};font-size:12px;color:{MID_BLUE};margin-top:5px;">
        {sub}
      </div>
    </td></tr>
  </table>
</td>
''')

    rows_html = ""
    for i in range(0, len(cells), 2):
        pair = cells[i:i+2]
        if len(pair) == 1:
            pair.append('<td width="50%">&nbsp;</td>')
        rows_html += f'<tr>{"".join(pair)}</tr>'

    return f'''
<tr><td class="eb-gutter-kpi" style="padding:16px 17px 6px;">
  <table cellpadding="0" cellspacing="0" border="0" width="100%">
    {rows_html}
  </table>
</td></tr>
'''


def _section_header_row(title: str, meta: str = "") -> str:
    meta_html = (
        f'<td align="right" class="eb-section-meta" style="font-family:{SANS};font-size:10px;color:{ACCENT_BLUE};'
        f'letter-spacing:0.14em;font-weight:400;">{_e(meta.upper())}</td>'
        if meta else ""
    )
    return f'''
<table cellpadding="0" cellspacing="0" border="0" width="100%"
       style="border-bottom:2px solid {GOLD};margin-bottom:10px;">
  <tr>
    <td class="eb-section-hdr" style="font-family:{SANS};font-size:11px;color:{NAVY};font-weight:700;
               letter-spacing:0.19em;padding:0 0 7px 0;">
      {_e(title.upper())}
    </td>
    {meta_html}
  </tr>
</table>
'''


def _data_table_block(title: str, meta: str, headers: List[str],
                      rows: List[List[Any]]) -> str:
    """One data section: section header + data table."""
    hdr_cells = []
    for i, h in enumerate(headers):
        align = "left" if i == 0 else "right"
        hdr_cells.append(
            f'<th align="{align}" class="eb-thead" style="font-family:{SANS};font-size:10px;color:{MID_BLUE};'
            f'font-weight:700;letter-spacing:0.08em;padding:7px 5px;'
            f'border-bottom:1px solid {SILVER};white-space:nowrap;">{_e(h.upper())}</th>'
        )

    body_rows = []
    for ri, row in enumerate(rows):
        bg = TINT if ri % 2 == 1 else WHITE
        cells = []
        for ci, cell in enumerate(row):
            if ci == 0:
                cells.append(
                    f'<td class="eb-cell-name" style="font-family:{SANS};font-size:12px;font-weight:700;color:{NAVY};'
                    f'padding:7px 5px;border-bottom:1px solid {BORDER};">{_e(cell)}</td>'
                )
            elif ci == 1:
                cells.append(
                    f'<td align="right" class="eb-cell-num" style="font-family:{SANS};font-size:12px;color:{NAVY};'
                    f'padding:7px 5px;border-bottom:1px solid {BORDER};white-space:nowrap;">{_e(cell)}</td>'
                )
            else:
                cells.append(
                    f'<td align="right" class="eb-cell-num" style="font-family:{SANS};font-size:12px;'
                    f'padding:7px 5px;border-bottom:1px solid {BORDER};white-space:nowrap;">'
                    f'{_pct_inline(cell)}</td>'
                )
        body_rows.append(f'<tr bgcolor="{bg}">{"".join(cells)}</tr>')

    return f'''
<tr><td class="eb-gutter" style="padding:8px 22px 6px;">
  <table cellpadding="0" cellspacing="0" border="0" width="100%"
         style="background:{WHITE};border:1px solid {BORDER};border-radius:8px;">
    <tr><td class="eb-card" style="padding:14px;">
      {_section_header_row(title, meta)}
      <table cellpadding="0" cellspacing="0" border="0" width="100%"
             style="border-collapse:collapse;">
        <thead><tr>{"".join(hdr_cells)}</tr></thead>
        <tbody>{"".join(body_rows)}</tbody>
      </table>
    </td></tr>
  </table>
</td></tr>
'''


def _section_rows(data: Dict[str, Any], sec: str) -> List[List[Any]]:
    out = []
    for r in data.get(sec, []):
        out.append([
            r.get("name", ""),
            _clean_px(r.get("px_last", "N/A"), code=r.get("code", "")),
            r.get("change_1d", "N/A"),
            r.get("mtd", "N/A"),
            r.get("ytd", "N/A"),
        ])
    return out


def _news_card_block(item: Dict[str, Any]) -> str:
    src      = str(item.get("source", "")).upper()
    headline = _e(item.get("headline", ""))
    summary  = _e(item.get("summary", ""))
    url      = (item.get("url") or "").strip()
    has_url  = bool(url and url.startswith(("http://", "https://")))

    if has_url:
        headline_html = (
            f'<a href="{_e(url)}" target="_blank" rel="noopener noreferrer" '
            f'style="color:{NAVY};text-decoration:none;">{headline}</a>'
        )
        more_html = (
            f'<div style="text-align:right;margin-top:10px;">'
            f'<a class="eb-news-more" href="{_e(url)}" target="_blank" rel="noopener noreferrer" '
            f'style="font-family:{SANS};font-size:12.5px;font-weight:700;'
            f'color:{ACCENT_BLUE};text-decoration:none;letter-spacing:0.04em;">'
            f'Read more &rarr;</a></div>'
        )
    else:
        headline_html = headline
        more_html = ""

    return f'''
<table cellpadding="0" cellspacing="0" border="0" width="100%"
       style="background:{WHITE};border:1px solid {BORDER};border-left:3px solid {GOLD};
              margin-bottom:10px;">
  <tr><td style="padding:12px 14px;">
    <div class="eb-news-source" style="font-family:{SANS};font-size:10.5px;font-weight:700;color:{ACCENT_BLUE};
                letter-spacing:0.16em;margin-bottom:5px;">{_e(src)}</div>
    <div class="eb-news-head" style="font-family:{SANS};font-size:16px;font-weight:700;color:{NAVY};
                line-height:1.3;margin-bottom:6px;">{headline_html}</div>
    <div class="eb-news-summ" style="font-family:{SANS};font-size:13px;color:{MID_BLUE};line-height:1.5;">
      {summary}
    </div>
    {more_html}
  </td></tr>
</table>
'''


def _news_section_block(title: str, meta: str, items: List[Dict[str, Any]]) -> str:
    if not items:
        cards_html = (
            f'<p style="font-family:{SANS};font-size:13px;color:{MUTED};font-style:italic;">'
            'No items.</p>'
        )
    else:
        cards_html = "".join(_news_card_block(it) for it in items)

    return f'''
<tr><td style="padding:14px 16px 4px;">
  {_section_header_row(title, meta)}
  {cards_html}
</td></tr>
'''


def _footer(report_date: str, report_title: str = "Market Intelligence") -> str:
    return f'''
<tr><td class="eb-gutter" style="background:{BRAND_BLUE};padding:16px 22px;
               border-top:3px solid {GOLD};">
  <table cellpadding="0" cellspacing="0" border="0" width="100%">
    <tr>
      <td style="font-family:{SERIF};font-size:12px;font-weight:700;color:{GOLD};
                 letter-spacing:0.14em;">DOHA BANK</td>
      <td align="right" style="font-family:{SANS};font-size:10px;color:{SILVER};
                               letter-spacing:0.14em;">
        {_e(report_title.upper())} &middot; {_e(report_date)}
      </td>
    </tr>
  </table>
</td></tr>
'''


# ============================================================
# Main builder
# ============================================================

def build_email_body(data: Dict[str, Any]) -> str:
    """
    Render the full daily report as an email-safe HTML body.
    Returns a single string ready to assign to the 'html' field of
    the email payload sent to Resend.
    """
    cfg                    = data.get("config", {})
    report_date            = cfg.get("report_date", "")
    report_title           = cfg.get("report_title", "Market Intelligence")
    logo_url               = cfg.get("logo_url") or DEFAULT_LOGO_URL
    generated_display_time = data.get("generated_display_time", "")

    # "Wednesday \u2022 22 July 2026" — same treatment as the browser HTML
    try:
        from datetime import datetime as _dtm
        _parsed = _dtm.strptime(report_date.strip(), "%d %B %Y")
        display_date = f"{_parsed.strftime('%A')} \u2022 {report_date.strip()}"
    except Exception:
        display_date = report_date

    sections = []
    sections.append(_masthead(display_date, generated_display_time,
                              logo_url, report_title))
    sections.append(_kpi_block(data.get("kpis", [])))

    # 7 data tables — same order as the browser HTML
    sections.append(_data_table_block(
        "Global Indices", "USD",
        ["Market / Index", "PX Last", "1D %", "MTD %", "YTD %"],
        _section_rows(data, "global_indices"),
    ))
    sections.append(_data_table_block(
        "GCC & Regional Indices", "LOCAL CCY",
        ["Market / Index", "PX Last", "1D %", "MTD %", "YTD %"],
        _section_rows(data, "gcc_indices"),
    ))
    sections.append(_data_table_block(
        "Spot Currency", "FX",
        ["Pair", "PX Last", "1D %", "MTD %", "YTD %"],
        _section_rows(data, "spot_currency"),
    ))
    sections.append(_data_table_block(
        "QAR Cross Rates", "QAR",
        ["Pair", "PX Last", "1D %", "MTD %", "YTD %"],
        _section_rows(data, "qar_cross_rates"),
    ))
    sections.append(_data_table_block(
        "Fixed Income · UST Yields", "YIELD %",
        ["Instrument", "Yield", "1D %", "MTD %", "YTD %"],
        _section_rows(data, "fixed_income"),
    ))
    sections.append(_data_table_block(
        "Qatari Banks", "QAR",
        ["Bank", "PX Last", "1D %", "MTD %", "YTD %"],
        _section_rows(data, "qatari_banks"),
    ))
    sections.append(_data_table_block(
        "Commodities & Energy", "USD",
        ["Asset", "PX Last", "1D %", "MTD %", "YTD %"],
        _section_rows(data, "commodities"),
    ))

    # News sections — keep the same counts the PDF uses
    sections.append(_news_section_block(
        "Regional & Global News", "",
        data.get("global_news", [])[:5],
    ))
    sections.append(_news_section_block(
        "Qatar News", "",
        data.get("qatar_news", [])[:4],
    ))
    sections.append(_news_section_block(
        "Market Drivers", "What Moved Markets",
        data.get("market_drivers", [])[:3],
    ))

    sections.append(_footer(display_date, report_title))

    inner = "".join(sections)

    # Desktop bump via @media query. Inline styles in the body are the
    # mobile/safe default — guaranteed to render on every client. Modern
    # clients (Gmail web/iOS/Android, Apple Mail Mac/iOS, Outlook 365 web)
    # honour this <style> block and apply the larger sizes; Outlook
    # desktop on Windows strips it and keeps the mobile sizing — no crash,
    # just no bump for those recipients.
    style_block = '''
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  /* Phones: let the 600px design reflow inside the screen instead of
     forcing a horizontal scroll. */
  @media only screen and (max-width: 480px) {
    .eb-gutter                  { padding-left: 12px !important; padding-right: 12px !important; }
    .eb-gutter-kpi              { padding-left: 7px !important;  padding-right: 7px !important; }
    .eb-card                    { padding: 10px !important; }
    .eb-cell-name, .eb-cell-num { font-size: 10.5px !important; padding: 6px 2px !important; }
    .eb-thead                   { font-size: 8.5px !important; padding: 6px 3px !important;
                                  letter-spacing: 0.04em !important; }
    .eb-title                   { font-size: 26px !important; }
    .eb-kpi-value               { font-size: 22px !important; }
    .eb-kpi-label               { font-size: 9px !important; letter-spacing: 0.10em !important; }
    .eb-kpi-sub                 { font-size: 10px !important; }
  }
  @media only screen and (min-width: 700px) {
    .eb-title       { font-size: 44px !important; }
    .eb-date        { font-size: 18px !important; }
    .eb-kpi-label   { font-size: 12px !important; }
    .eb-kpi-value   { font-size: 34px !important; }
    .eb-kpi-sub     { font-size: 14px !important; }
    .eb-section-hdr { font-size: 13px !important; }
    .eb-section-meta{ font-size: 11.5px !important; }
    .eb-thead       { font-size: 11.5px !important; }
    .eb-cell-name   { font-size: 15px !important; }
    .eb-cell-num    { font-size: 15px !important; }
    .eb-news-source { font-size: 13px !important; }
    .eb-news-head   { font-size: 20px !important; }
    .eb-news-summ   { font-size: 15px !important; line-height: 1.55 !important; }
    .eb-news-more   { font-size: 14px !important; }
  }
</style>
'''

    # Outer container: 600px max, centred, light page background. The HTML
    # shell is minimal because Resend / clients wrap their own boilerplate.
    return style_block + f'''
<table cellpadding="0" cellspacing="0" border="0" width="100%"
       style="background:{PAGE_BG};padding:8px 0;">
  <tr><td align="center">
    <table cellpadding="0" cellspacing="0" border="0" width="600"
           style="max-width:600px;width:100%;background:{PAGE_BG};">
      {inner}
    </table>
  </td></tr>
</table>
'''


if __name__ == "__main__":
    import json
    import sys
    if len(sys.argv) < 2:
        print("Usage: python email_body_generator.py market_data.json [output.html]")
        raise SystemExit(1)
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        data = json.load(f)
    body = build_email_body(data)
    out_path = sys.argv[2] if len(sys.argv) >= 3 else "email_body.html"
    with open(out_path, "w", encoding="utf-8") as f:
        # Wrap in minimal full-HTML scaffolding for standalone preview
        f.write(f'<!doctype html><html><body>{body}</body></html>')
    print(f"Email body written to {out_path} ({len(body)} chars)")

    

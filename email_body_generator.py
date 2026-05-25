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
from typing import Any, Dict, List


# ============================================================
# Brand palette — hardcoded; email clients ignore CSS variables
# ============================================================

BRAND_BLUE  = "#1B5FA5"
SKY_BLUE    = "#38B6FF"
NAVY        = "#0A2540"
MID_BLUE    = "#4677B0"
ACCENT_BLUE = "#6595CB"
LIGHT_BLUE  = "#9EBEDF"
MUTED       = "#88A5C2"
WHITE       = "#FFFFFF"
TINT        = "#F5F9FC"
KPI_BG      = "#F1F6FB"
BORDER      = "#E8F0F8"
PAGE_BG     = "#FBFCFE"

SERIF = "Georgia, 'Times New Roman', serif"
SANS  = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"


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

def _masthead(report_date: str, generated_display_time: str) -> str:
    return f'''
<tr><td style="background:{BRAND_BLUE};padding:18px 22px 22px;">
  <table cellpadding="0" cellspacing="0" border="0" width="100%">
    <tr>
      <td style="font-family:{SANS};color:{SKY_BLUE};font-size:11px;font-weight:700;letter-spacing:0.28em;">
        DOHA BANK
      </td>
      <td align="right" style="font-family:{SANS};color:#9EBEDF;font-size:10px;letter-spacing:0.14em;">
        {_e(generated_display_time)}
      </td>
    </tr>
    <tr><td colspan="2" height="10" style="line-height:0;font-size:0;">&nbsp;</td></tr>
    <tr>
      <td class="eb-title" style="font-family:{SERIF};color:{WHITE};font-size:30px;font-weight:700;line-height:1.05;">
        Market Updates
      </td>
      <td align="right" class="eb-date" style="font-family:{SERIF};color:#C5DCEF;font-size:14px;font-style:italic;">
        {_e(report_date)}
      </td>
    </tr>
  </table>
</td></tr>
'''


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
<td valign="top" style="padding:6px;" width="50%">
  <table cellpadding="0" cellspacing="0" border="0" width="100%"
         style="background:{KPI_BG};border-left:3px solid {BRAND_BLUE};">
    <tr><td style="padding:12px 14px;">
      <div class="eb-kpi-label" style="font-family:{SANS};font-size:10.5px;font-weight:700;color:{MID_BLUE};letter-spacing:0.16em;">
        {_e(label)}
      </div>
      <div class="eb-kpi-value" style="font-family:{SERIF};font-size:24px;font-weight:700;color:{NAVY};margin-top:4px;line-height:1.1;">
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
<tr><td style="padding:18px 16px 8px;">
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
       style="border-bottom:1.5px solid {BRAND_BLUE};margin-bottom:6px;">
  <tr>
    <td class="eb-section-hdr" style="font-family:{SANS};font-size:11px;color:{NAVY};font-weight:700;
               letter-spacing:0.22em;padding:0 0 6px 0;">
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
            f'<th align="{align}" class="eb-thead" style="font-family:{SANS};font-size:10px;color:{ACCENT_BLUE};'
            f'font-weight:700;letter-spacing:0.14em;padding:8px 10px;'
            f'border-bottom:1px solid {BORDER};white-space:nowrap;">{_e(h.upper())}</th>'
        )

    body_rows = []
    for ri, row in enumerate(rows):
        bg = TINT if ri % 2 == 1 else WHITE
        cells = []
        for ci, cell in enumerate(row):
            if ci == 0:
                cells.append(
                    f'<td class="eb-cell-name" style="font-family:{SANS};font-size:13px;font-weight:700;color:{NAVY};'
                    f'padding:8px 10px;border-bottom:1px solid {BORDER};">{_e(cell)}</td>'
                )
            elif ci == 1:
                cells.append(
                    f'<td align="right" class="eb-cell-num" style="font-family:{SANS};font-size:13px;color:{NAVY};'
                    f'padding:8px 10px;border-bottom:1px solid {BORDER};white-space:nowrap;">{_e(cell)}</td>'
                )
            else:
                cells.append(
                    f'<td align="right" class="eb-cell-num" style="font-family:{SANS};font-size:13px;'
                    f'padding:8px 10px;border-bottom:1px solid {BORDER};white-space:nowrap;">'
                    f'{_pct_inline(cell)}</td>'
                )
        body_rows.append(f'<tr bgcolor="{bg}">{"".join(cells)}</tr>')

    return f'''
<tr><td style="padding:14px 16px 4px;">
  {_section_header_row(title, meta)}
  <table cellpadding="0" cellspacing="0" border="0" width="100%"
         style="border-collapse:collapse;">
    <thead><tr>{"".join(hdr_cells)}</tr></thead>
    <tbody>{"".join(body_rows)}</tbody>
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
       style="background:{WHITE};border:1px solid {BORDER};border-left:3px solid {BRAND_BLUE};
              margin-bottom:10px;">
  <tr><td style="padding:12px 14px;">
    <div class="eb-news-source" style="font-family:{SANS};font-size:10.5px;font-weight:700;color:{ACCENT_BLUE};
                letter-spacing:0.16em;margin-bottom:5px;">{_e(src)}</div>
    <div class="eb-news-head" style="font-family:{SERIF};font-size:16px;font-weight:700;color:{NAVY};
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


def _footer(report_date: str) -> str:
    return f'''
<tr><td style="background:{BRAND_BLUE};padding:14px 18px;">
  <table cellpadding="0" cellspacing="0" border="0" width="100%">
    <tr>
      <td style="font-family:{SANS};font-size:11px;font-weight:700;color:{SKY_BLUE};
                 letter-spacing:0.24em;">DOHA BANK</td>
      <td align="right" style="font-family:{SANS};font-size:10px;color:#9EBEDF;
                               letter-spacing:0.14em;">
        MARKET UPDATES &middot; {_e(report_date)}
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
    report_date            = data.get("config", {}).get("report_date", "")
    generated_display_time = data.get("generated_display_time", "")

    sections = []
    sections.append(_masthead(report_date, generated_display_time))
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
        data.get("global_news", [])[:6],
    ))
    sections.append(_news_section_block(
        "Qatar News", "",
        data.get("qatar_news", [])[:4],
    ))
    sections.append(_news_section_block(
        "Market Drivers", "What Moved Markets",
        data.get("market_drivers", [])[:4],
    ))

    sections.append(_footer(report_date))

    inner = "".join(sections)

    # Desktop bump via @media query. Inline styles in the body are the
    # mobile/safe default — guaranteed to render on every client. Modern
    # clients (Gmail web/iOS/Android, Apple Mail Mac/iOS, Outlook 365 web)
    # honour this <style> block and apply the larger sizes; Outlook
    # desktop on Windows strips it and keeps the mobile sizing — no crash,
    # just no bump for those recipients.
    style_block = '''
<style>
  @media only screen and (min-width: 700px) {
    .eb-title       { font-size: 38px !important; }
    .eb-date        { font-size: 18px !important; }
    .eb-kpi-label   { font-size: 12px !important; }
    .eb-kpi-value   { font-size: 30px !important; }
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

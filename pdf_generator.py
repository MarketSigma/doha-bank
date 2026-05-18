"""
Doha Bank Market Updates - PDF Generator
========================================

Monochrome blue editorial design:
  - Brand blue (#1B5FA5) header band, section underlines, footer
  - "DOHA BANK" wordmark in bright sky blue (#38B6FF) — matches the logo
  - Dark navy (#0A2540) for all data text and big numbers
  - Direction encoded by glyph weight (▲▼●), not hue
        ▲ in navy        gains
        ▼ in light blue  declines
        ● in accent blue neutral / pegged
  - Serif (Caladea) for headlines + KPI numbers
  - Sans (Carlito) for tabular data and tracked-uppercase labels
"""

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas as pdfcanvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os
import sys
import json
from datetime import date as dt


# ============================================================
# Font registration
# ============================================================

def register_fonts():
    font_paths = {
        "Caladea":        "/usr/share/fonts/truetype/crosextra/Caladea-Regular.ttf",
        "Caladea-Bold":   "/usr/share/fonts/truetype/crosextra/Caladea-Bold.ttf",
        "Caladea-Italic": "/usr/share/fonts/truetype/crosextra/Caladea-Italic.ttf",
        "Carlito":        "/usr/share/fonts/truetype/crosextra/Carlito-Regular.ttf",
        "Carlito-Bold":   "/usr/share/fonts/truetype/crosextra/Carlito-Bold.ttf",
        "Carlito-Italic": "/usr/share/fonts/truetype/crosextra/Carlito-Italic.ttf",
        # DejaVu has the ▲ ▼ ● glyphs Carlito lacks. Used only for direction markers.
        "Symbol":         "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "Symbol-Bold":    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    }
    for font_name, path in font_paths.items():
        if os.path.exists(path):
            pdfmetrics.registerFont(TTFont(font_name, path))


register_fonts()


# ============================================================
# Page geometry
# ============================================================

W, H = landscape(A4)
M    = 11 * mm           # outer margin
UW   = W - 2 * M         # usable width


# ============================================================
# Monochrome blue palette
# ------------------------------------------------------------
# A single-hue stack. All gradations of blue. No green / red / gold.
# ============================================================

# Brand chrome
BRAND_BLUE  = colors.HexColor("#1B5FA5")  # header band, section rules, footer
SKY_BLUE    = colors.HexColor("#38B6FF")  # "DOHA BANK" wordmark — logo blue
HDR_SUB     = colors.HexColor("#C5DCEF")  # subtitle on header band
HDR_META    = colors.HexColor("#9EBEDF")  # page meta on header band

# Text & data
NAVY        = colors.HexColor("#0A2540")  # primary text, big numbers, ▲ glyph
MID_BLUE    = colors.HexColor("#4677B0")  # secondary text, declined values
ACCENT_BLUE = colors.HexColor("#6595CB")  # column headers, neutral ● glyph
LIGHT_BLUE  = colors.HexColor("#9EBEDF")  # ▼ glyph
MUTED       = colors.HexColor("#88A5C2")  # N/A placeholders

# Surfaces
WHITE       = colors.white
TINT        = colors.HexColor("#F5F9FC")  # alternating row tint
KPI_BG      = colors.HexColor("#F1F6FB")  # KPI card fill
BORDER      = colors.HexColor("#E8F0F8")  # hairline borders


# ============================================================
# Layout constants
# ============================================================

HEADER_H  = 25 * mm
FTR_H     = 5  * mm
KPI_H     = 13 * mm     # tighter — content was leaving empty space at bottom
SEC_H     = 5.5 * mm
TBL_HDR_H = 4.2 * mm
ROW_H     = 4.2 * mm
GAP       = 3 * mm
NEWS_CARD_H = 25 * mm   # fixed news card height; cards fit content, no stretching


# ============================================================
# Drawing primitives
# ============================================================

def fr(c, x, y, w, h, col):
    """Filled rectangle."""
    c.setFillColor(col)
    c.rect(x, y, w, h, fill=1, stroke=0)


def sr(c, x, y, w, h, col, lw=0.4):
    """Stroked rectangle."""
    c.setStrokeColor(col)
    c.setLineWidth(lw)
    c.rect(x, y, w, h, fill=0, stroke=1)


def hl(c, x1, y, x2, col=BORDER, lw=0.35):
    """Horizontal rule."""
    c.setStrokeColor(col)
    c.setLineWidth(lw)
    c.line(x1, y, x2, y)


def t(c, txt, x, y, font="Carlito", size=8, color=NAVY,
      align="left", maxw=None, tracking=0):
    """
    Draw text. Optional `tracking` (in points) applies letter-spacing
    for uppercase labels (~1.2-2.5 points reads as 'tracked').

    Uses canvas.beginText() when tracking is requested because the
    canvas itself doesn't expose setCharSpace publicly.
    """
    txt = "" if txt is None else str(txt)
    c.setFont(font, size)
    c.setFillColor(color)

    if maxw:
        while len(txt) > 4 and c.stringWidth(txt, font, size) > maxw:
            txt = txt[:-4] + "..."

    if tracking:
        # Width including extra inter-character spacing for alignment math
        text_w = c.stringWidth(txt, font, size) + tracking * max(0, len(txt) - 1)
        if align == "right":
            start_x = x - text_w
        elif align == "center":
            start_x = x - text_w / 2
        else:
            start_x = x

        to = c.beginText(start_x, y)
        to.setFont(font, size)
        to.setFillColor(color)
        to.setCharSpace(tracking)
        to.textOut(txt)
        # Reset char spacing so it doesn't leak into subsequent drawString calls
        to.setCharSpace(0)
        c.drawText(to)
        return

    if align == "right":
        c.drawRightString(x, y, txt)
    elif align == "center":
        c.drawCentredString(x, y, txt)
    else:
        c.drawString(x, y, txt)


def ml(c, txt, x, y, font, size, color, maxw, lh, maxl=3):
    """Word-wrapped text. Returns y after the last drawn line."""
    txt = "" if txt is None else str(txt)
    c.setFont(font, size)
    c.setFillColor(color)
    words = txt.split()
    lines, line = [], ""
    for w in words:
        cand = (line + " " + w).strip()
        if c.stringWidth(cand, font, size) <= maxw:
            line = cand
        else:
            if line:
                lines.append(line)
            line = w
            if len(lines) >= maxl:
                break
    if line and len(lines) < maxl:
        lines.append(line)
    for i, line_text in enumerate(lines[:maxl]):
        c.drawString(x, y - i * lh, line_text)
    return y - len(lines[:maxl]) * lh


# ============================================================
# Percentage rendering — monochrome blue, no red/green
# ------------------------------------------------------------
# Direction is shown by glyph weight, not hue:
#   ▲ NAVY (dark)        — gain
#   ▼ LIGHT_BLUE         — decline
#   ● ACCENT_BLUE        — neutral (pegged / 0.00%)
# Number weight reinforces direction:
#   gains  → NAVY bold
#   losses → MID_BLUE regular
# ============================================================

def _parse_pct(value):
    value = str(value or "").strip()
    # Check zero-magnitude values first (regardless of any leading +/-)
    if value in ("Pegged", "0.00%", "0%", "+0.00%", "-0.00%", "+0%", "-0%"):
        display = "Pegged" if value == "Pegged" else "0.00%"
        return ("●", ACCENT_BLUE, display,           MID_BLUE, "Carlito")
    if value.startswith("+"):
        return ("▲", NAVY,        value.lstrip("+"), NAVY,     "Carlito-Bold")
    if value.startswith("-"):
        return ("▼", LIGHT_BLUE,  value.lstrip("-"), MID_BLUE, "Carlito")
    if value in ("", "N/A", "n/a", "—"):
        return ("",  None,        "N/A",             MUTED,    "Carlito")
    return ("", None, value, NAVY, "Carlito")


def draw_pct(c, right_x, y, value, size=7.2):
    """Render percentage right-aligned at right_x with directional glyph."""
    glyph, glyph_color, num, text_color, font = _parse_pct(value)
    # Number in the chosen body font (Carlito / Carlito-Bold)
    c.setFillColor(text_color)
    c.setFont(font, size)
    c.drawRightString(right_x, y, num)
    # Glyph in DejaVu Sans Bold — thicker strokes survive small-size rasterization
    if glyph and glyph_color:
        num_w = c.stringWidth(num, font, size)
        glyph_size = size - 0.4
        c.setFillColor(glyph_color)
        c.setFont("Symbol-Bold", glyph_size)
        c.drawRightString(right_x - num_w - 1.4, y, glyph)


# ============================================================
# Data formatting helpers (unchanged from v1)
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


def safe_text(value, default="N/A"):
    if value is None:
        return default
    value = str(value)
    return value if value.strip() else default


def cw5(w):
    return [w * 0.37, w * 0.18, w * 0.15, w * 0.15, w * 0.15]


# ============================================================
# Header & footer
# ============================================================

def draw_header(c, report_date, generated_display_time,
                market_as_of_date=None, page=1, total=2, report_status="PASS"):
    """
    Editorial masthead — two tiers separated by a thin sky-blue rule.
      Upper tier: DOHA BANK wordmark (left) · page meta (right)
      Lower tier: Market Updates (large serif, left) · report date (serif italic, right)
    """
    fr(c, 0, H - HEADER_H, W, HEADER_H, BRAND_BLUE)

    # --- Upper tier: chrome row ---
    t(c, "DOHA BANK", M, H - 7 * mm,
      "Carlito-Bold", 9, SKY_BLUE, tracking=3)

    meta_text = f"PAGE {page} / {total}  \u00b7  {generated_display_time}"
    t(c, meta_text, W - M, H - 7 * mm,
      "Carlito", 7, HDR_META, "right", tracking=1.5)

    # --- Lower tier: title + date on the same baseline ---
    t(c, "Market Updates", M, H - 20 * mm,
      "Caladea-Bold", 26, WHITE)

    t(c, report_date, W - M, H - 20 * mm,
      "Caladea-Italic", 13, HDR_SUB, "right")

    return H - HEADER_H - 1.5 * mm


def draw_footer(c, report_date):
    """Brand-blue footer band with brand mark left, tracked caption right."""
    fr(c, 0, 0, W, FTR_H, BRAND_BLUE)
    # Left: small sky-blue wordmark echo, ties to the header
    t(c, "DOHA BANK", M, 1.8 * mm, "Carlito-Bold", 6, SKY_BLUE, tracking=2)
    # Right: tracked caption
    t(c, f"MARKET UPDATES  \u00b7  {report_date}",
      W - M, 1.8 * mm, "Carlito", 5.5, HDR_META, "right", tracking=1.5)


# ============================================================
# KPI strip — pale-blue cards, serif navy numbers
# ============================================================

def draw_kpi(c, y, kpis):
    if not kpis:
        return y

    gap = 2.5 * mm
    cw  = (UW - gap * (len(kpis) - 1)) / len(kpis)

    for i, k in enumerate(kpis):
        val = str(k.get("value", "—"))
        lbl = str(k.get("label", ""))
        sub = str(k.get("sublabel", ""))
        cx  = M + i * (cw + gap)

        # Card surface
        fr(c, cx, y - KPI_H, cw, KPI_H, KPI_BG)

        # Left accent rule — thin brand-blue marker on the leading edge
        fr(c, cx, y - KPI_H, 0.7 * mm, KPI_H, BRAND_BLUE)

        # Tracked uppercase label
        t(c, lbl.upper(), cx + 3.5 * mm, y - 3.2 * mm,
          "Carlito-Bold", 6.5, MID_BLUE, tracking=1.6)

        # Big serif number
        t(c, val, cx + 3.5 * mm, y - 8 * mm,
          "Caladea-Bold", 14, NAVY)

        # Change line — leading glyph + sub text
        sub_clean = sub
        glyph, glyph_color = "", None
        first_token = sub.split()[0] if sub else ""
        if first_token in ("0.00%", "+0.00%", "-0.00%", "0%", "+0%", "-0%", "Pegged"):
            glyph, glyph_color = "●", ACCENT_BLUE
            sub_clean = sub.lstrip("+-")
        elif sub.startswith("+"):
            glyph, glyph_color = "▲", NAVY
            sub_clean = sub.lstrip("+")
        elif sub.startswith("-"):
            glyph, glyph_color = "▼", LIGHT_BLUE
            sub_clean = sub.lstrip("-")

        text_x = cx + 3.5 * mm
        if glyph:
            c.setFillColor(glyph_color)
            c.setFont("Symbol-Bold", 6.5)
            c.drawString(text_x, y - 11.3 * mm, glyph)
            text_x += 2.8 * mm
        t(c, sub_clean, text_x, y - 11.3 * mm, "Carlito", 6.8, MID_BLUE)

    return y - KPI_H - 3 * mm


# ============================================================
# Section header — tracked uppercase + thin brand-blue rule
# ============================================================

def sec_hdr(c, x, y, title, w, meta=None):
    """
    Section header: tracked uppercase navy label on the left,
    optional tracked meta tag on the right (e.g. "USD", "BPS"),
    thin brand-blue rule beneath. Returns y where the table begins.
    """
    label_baseline = y - 3.5 * mm
    t(c, title.upper(), x, label_baseline,
      "Carlito-Bold", 7, NAVY, tracking=2.2)
    if meta:
        t(c, meta.upper(), x + w, label_baseline,
          "Carlito", 6, ACCENT_BLUE, "right", tracking=1.5)
    rule_y = label_baseline - 1.4 * mm
    hl(c, x, rule_y, x + w, BRAND_BLUE, 1.2)
    return rule_y - 1 * mm


# ============================================================
# Tables
# ============================================================

def draw_table(c, x, y, hdrs, rows, tw, cws):
    """
    Editorial table:
      - column headers tracked uppercase accent blue (no fill)
      - hairline underline beneath
      - subtle alternating row tint
      - right-aligned percentages with directional blue glyphs
    """
    # Column headers
    cx = x
    for i, (h, cw) in enumerate(zip(hdrs, cws)):
        hy = y - TBL_HDR_H + 1.3 * mm
        if i == 0:
            t(c, h.upper(), cx + 2 * mm, hy,
              "Carlito-Bold", 6.5, ACCENT_BLUE, tracking=1.3)
        else:
            t(c, h.upper(), cx + cw - 1.5 * mm, hy,
              "Carlito-Bold", 6.5, ACCENT_BLUE, "right", tracking=1.3)
        cx += cw

    y -= TBL_HDR_H
    hl(c, x, y, x + tw, BORDER, 0.6)

    # Data rows
    for ri, row in enumerate(rows):
        if ri % 2 == 1:
            fr(c, x, y - ROW_H, tw, ROW_H, TINT)

        cx = x
        cell_y = y - ROW_H + 1.4 * mm

        for ci, (cell, cw) in enumerate(zip(row, cws)):
            cell_text = safe_text(cell)
            if ci == 0:
                t(c, cell_text, cx + 2 * mm, cell_y,
                  "Carlito-Bold", 7.2, NAVY, maxw=cw - 3 * mm)
            elif ci == 1:
                t(c, cell_text, cx + cw - 1.5 * mm, cell_y,
                  "Carlito", 7.2, NAVY, "right")
            else:
                draw_pct(c, cx + cw - 1.5 * mm, cell_y, cell_text, size=7.2)
            cx += cw

        y -= ROW_H

    return y - 1.5 * mm


def section_rows(data, sec):
    out = []
    for r in data.get(sec, []):
        px   = r.get("px_last", "N/A")
        code = r.get("code", "")
        name = r.get("name", "")
        out.append([
            name,
            clean_px(px, code=code, name=name),
            r.get("change_1d", "N/A"),
            r.get("mtd", "N/A"),
            r.get("ytd", "N/A"),
        ])
    return out


# ============================================================
# News cards
# ============================================================

def draw_news_card(c, x, y, w, h, item):
    """
    Editorial news card:
      - hairline border, white surface
      - thin brand-blue accent rule along the left edge
      - tracked source tag → serif navy headline → mid-blue body
    """
    src    = str(item.get("source", ""))
    hl_txt = str(item.get("headline", ""))
    summ   = str(item.get("summary", ""))

    fr(c, x, y - h, w, h, WHITE)
    sr(c, x, y - h, w, h, BORDER, 0.5)

    # Thin brand-blue accent along the left edge
    fr(c, x, y - h, 0.7 * mm, h, BRAND_BLUE)

    pad     = 3.5 * mm
    inner_w = w - 2 * pad

    # Source tag — tracked uppercase accent blue
    t(c, src.upper(), x + pad, y - 4 * mm,
      "Carlito-Bold", 6, ACCENT_BLUE, tracking=1.5)

    # Headline — serif navy, up to 2 wrapped lines
    headline_bottom = ml(c, hl_txt, x + pad, y - 7.5 * mm,
                        "Caladea-Bold", 9, NAVY,
                        inner_w, 3.4 * mm, maxl=2)

    # Summary — sans mid-blue, up to 3 wrapped lines (tighter for editorial feel)
    ml(c, summ, x + pad, headline_bottom - 1.8 * mm,
       "Carlito", 6.8, MID_BLUE,
       inner_w, 2.7 * mm, maxl=3)


def draw_news_grid(c, x, y, title, items, total_w, rows_count, card_h, card_gap=2 * mm, meta=None):
    y = sec_hdr(c, x, y, title, total_w, meta=meta)
    y -= 1.5 * mm

    card_w = (total_w - card_gap) / 2
    max_cards = rows_count * 2

    for i, item in enumerate(items[:max_cards]):
        row = i // 2
        col = i % 2
        cx  = x + col * (card_w + card_gap)
        cy  = y - row * (card_h + card_gap)
        draw_news_card(c, cx, cy, card_w, card_h, item)

    return y - rows_count * card_h - max(0, rows_count - 1) * card_gap


# ============================================================
# Pages
# ============================================================

def page1(c, report_date, generated_display_time, market_as_of_date, data):
    fr(c, 0, 0, W, H, WHITE)

    top = draw_header(
        c, report_date, generated_display_time,
        market_as_of_date=market_as_of_date,
        page=1, total=2,
        report_status=data.get("report_status", "ok"),
    )

    y = top - 2 * mm
    y = draw_kpi(c, y, data.get("kpis", []))

    cw2 = (UW - 4 * mm) / 2
    xL  = M
    xR  = M + cw2 + 4 * mm

    # Left column
    yL = y
    yL = sec_hdr(c, xL, yL, "Global Indices", cw2, meta="USD")
    yL = draw_table(c, xL, yL,
                    ["Market / Index", "PX Last", "1D %", "MTD %", "YTD %"],
                    section_rows(data, "global_indices"), cw2, cw5(cw2))
    yL -= GAP

    yL = sec_hdr(c, xL, yL, "Spot Currency", cw2, meta="FX")
    yL = draw_table(c, xL, yL,
                    ["Pair", "PX Last", "1D %", "MTD %", "YTD %"],
                    section_rows(data, "spot_currency"), cw2, cw5(cw2))
    yL -= GAP

    yL = sec_hdr(c, xL, yL, "QAR Cross Rates", cw2, meta="QAR")
    yL = draw_table(c, xL, yL,
                    ["Pair", "PX Last", "1D %", "MTD %", "YTD %"],
                    section_rows(data, "qar_cross_rates"), cw2, cw5(cw2))
    yL -= GAP

    yL = sec_hdr(c, xL, yL, "Fixed Income \u00b7 UST Yields", cw2, meta="YIELD %")
    yL = draw_table(c, xL, yL,
                    ["Instrument", "Yield", "1D %", "MTD %", "YTD %"],
                    section_rows(data, "fixed_income"), cw2, cw5(cw2))

    # Right column
    yR = y
    yR = sec_hdr(c, xR, yR, "GCC & Regional Indices", cw2, meta="LOCAL CCY")
    yR = draw_table(c, xR, yR,
                    ["Market / Index", "PX Last", "1D %", "MTD %", "YTD %"],
                    section_rows(data, "gcc_indices"), cw2, cw5(cw2))
    yR -= GAP

    yR = sec_hdr(c, xR, yR, "Qatari Banks", cw2, meta="QAR")
    yR = draw_table(c, xR, yR,
                    ["Bank", "PX Last", "1D %", "MTD %", "YTD %"],
                    section_rows(data, "qatari_banks"), cw2, cw5(cw2))
    yR -= GAP

    yR = sec_hdr(c, xR, yR, "Commodities & Energy", cw2, meta="USD")
    yR = draw_table(c, xR, yR,
                    ["Asset", "PX Last", "1D %", "MTD %", "YTD %"],
                    section_rows(data, "commodities"), cw2, cw5(cw2))

    draw_footer(c, report_date)


def page2(c, report_date, generated_display_time, market_as_of_date,
          global_news, qatar_news, report_status="ok"):
    fr(c, 0, 0, W, H, WHITE)

    top = draw_header(
        c, report_date, generated_display_time,
        market_as_of_date=market_as_of_date,
        page=2, total=2,
        report_status=report_status,
    )

    y = top - 2 * mm
    card_gap         = 2.5 * mm
    between_sections = 4 * mm

    y = draw_news_grid(
        c, M, y, "Regional & Global News", global_news, UW,
        rows_count=3, card_h=NEWS_CARD_H, card_gap=card_gap,
        meta=f"AS OF {generated_display_time}",
    )

    y -= between_sections

    draw_news_grid(
        c, M, y, "Qatar News", qatar_news, UW,
        rows_count=3, card_h=NEWS_CARD_H, card_gap=card_gap,
        meta=f"AS OF {generated_display_time}",
    )

    draw_footer(c, report_date)


# ============================================================
# Main generator
# ============================================================

def generate(data, output_path):
    report_date            = data.get("config", {}).get("report_date", dt.today().strftime("%d %B %Y"))
    generated_display_time = data.get("generated_display_time", "07:00 AST")
    market_as_of_date      = data.get("market_as_of_date")
    report_status          = data.get("report_status", "PASS")

    c = pdfcanvas.Canvas(output_path, pagesize=landscape(A4))
    c.setTitle(f"Doha Bank Market Updates – Snapshot & Key News - {report_date}")
    c.setAuthor("Doha Bank - AI-generated Daily Briefs")

    page1(c, report_date, generated_display_time, market_as_of_date, data)
    c.showPage()

    page2(
        c, report_date, generated_display_time, market_as_of_date,
        data.get("global_news", []),
        data.get("qatar_news", []),
        report_status,
    )
    c.showPage()

    c.save()

    size_bytes = os.path.getsize(output_path)
    print(f"PDF: {output_path}  |  {size_bytes / 1024:.0f} KB")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        data_path = sys.argv[1]
        out_path  = sys.argv[2]
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        generate(data, out_path)
    else:
        print("Usage: python pdf_generator.py market_data.json report.pdf")
        raise SystemExit(1)

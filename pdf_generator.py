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


# ------------------------------------------------------------
# Font registration
# ------------------------------------------------------------

def register_fonts():
    font_paths = {
        "Caladea": "/usr/share/fonts/truetype/crosextra/Caladea-Regular.ttf",
        "Caladea-Bold": "/usr/share/fonts/truetype/crosextra/Caladea-Bold.ttf",
        "Caladea-Italic": "/usr/share/fonts/truetype/crosextra/Caladea-Italic.ttf",
        "Carlito": "/usr/share/fonts/truetype/crosextra/Carlito-Regular.ttf",
        "Carlito-Bold": "/usr/share/fonts/truetype/crosextra/Carlito-Bold.ttf",
        "Carlito-Italic": "/usr/share/fonts/truetype/crosextra/Carlito-Italic.ttf",
    }

    for font_name, path in font_paths.items():
        if os.path.exists(path):
            pdfmetrics.registerFont(TTFont(font_name, path))


register_fonts()


# ------------------------------------------------------------
# Page constants
# ------------------------------------------------------------

W, H = landscape(A4)
M = 11 * mm
UW = W - 2 * M

BLUE = colors.HexColor("#1a5fa8")
NAVY = colors.HexColor("#0d2c5e")
CYAN = colors.HexColor("#00aeef")
GOLD = colors.HexColor("#c9a84c")
WHITE = colors.white
OFFWHT = colors.HexColor("#f4f8fd")
TBLHDR = colors.HexColor("#0f3d7a")
RULE = colors.HexColor("#c5d8ee")
RULE_DK = colors.HexColor("#7aafd4")
TEXT = colors.HexColor("#1a2a3a")
MUTED = colors.HexColor("#5a7a96")
UP = colors.HexColor("#1a7a45")
DOWN = colors.HexColor("#c0392b")
SUBT = colors.HexColor("#9ac4e8")
WARN = colors.HexColor("#b45309")

HDR_H = 24 * mm
FTR_H = 5.5 * mm
KPI_H = 14 * mm
SEC_H = 5.5 * mm
ROW_H = 4.6 * mm
GAP = 2.5 * mm


# ------------------------------------------------------------
# Basic drawing helpers
# ------------------------------------------------------------

def pct_col(v):
    v = str(v or "").strip()
    if v.startswith("+"):
        return UP
    if v.startswith("-"):
        return DOWN
    if v in ("N/A", "—", "-", ""):
        return MUTED
    return MUTED


def fr(c, x, y, w, h, col):
    c.setFillColor(col)
    c.rect(x, y, w, h, fill=1, stroke=0)


def sr(c, x, y, w, h, col, lw=0.4):
    c.setStrokeColor(col)
    c.setLineWidth(lw)
    c.rect(x, y, w, h, fill=0, stroke=1)


def t(c, txt, x, y, font="Carlito", size=8, color=TEXT, align="left", maxw=None):
    txt = "" if txt is None else str(txt)
    c.setFont(font, size)
    c.setFillColor(color)

    if maxw:
        while len(txt) > 4 and c.stringWidth(txt, font, size) > maxw:
            txt = txt[:-4] + "..."

    if align == "right":
        c.drawRightString(x, y, txt)
    elif align == "center":
        c.drawCentredString(x, y, txt)
    else:
        c.drawString(x, y, txt)


def hl(c, x1, y, x2, col=RULE, lw=0.35):
    c.setStrokeColor(col)
    c.setLineWidth(lw)
    c.line(x1, y, x2, y)


def ml(c, txt, x, y, font, size, color, maxw, lh, maxl=3):
    txt = "" if txt is None else str(txt)
    c.setFont(font, size)
    c.setFillColor(color)

    words = txt.split()
    lines = []
    line = ""

    for w in words:
        candidate = (line + " " + w).strip()
        if c.stringWidth(candidate, font, size) <= maxw:
            line = candidate
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


# ------------------------------------------------------------
# Formatting helpers
# ------------------------------------------------------------

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
    name = str(name or "")

    # FX and QAR cross rates need precision.
    if code in {
        "EURUSD",
        "GBPUSD",
        "USDCNY",
        "USDQAR",
        "EURQAR",
        "GBPQAR",
        "CNYQAR",
    }:
        return f"{value:,.4f}"

    # USD/JPY is a major FX pair but conventionally shown around 2 decimals.
    if code == "USDJPY":
        return f"{value:,.2f}"

    # US Treasury yields should keep precision.
    if code in {"UST5Y", "UST10Y"}:
        return f"{value:,.4f}"

    # Qatari bank stocks often need three decimals.
    if code in {"DHBK", "CBQK", "MARK", "DUBK", "ABQK", "QIIB"}:
        return f"{value:,.3f}"

    if code in {"QNBK", "QIBK"}:
        return f"{value:,.2f}"

    # Gold in QAR and large indices.
    if abs(value) >= 1000:
        return f"{value:,.2f}"

    # Commodities and normal index values below 1000.
    if abs(value) >= 10:
        return f"{value:,.2f}"

    # Small values.
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


# ------------------------------------------------------------
# Header and footer
# ------------------------------------------------------------

def draw_header(c, report_date, generated_display_time, market_as_of_date=None, page=1, total=2, report_status="PASS"):

    HEADER_H = 34 * mm

    BLUE_TOP = colors.HexColor("#165A9D")
    BLUE_BOTTOM = colors.HexColor("#1E6DB7")
    DATE_BLUE = colors.HexColor("#36A9E1")
    SOFT_WHITE = colors.HexColor("#EAF4FF")
    GLOW_BLUE = colors.HexColor("#62C7F5")

    # ------------------------------------------------------------
    # Main blue header background
    # ------------------------------------------------------------

    fr(c, 0, H - HEADER_H, W, HEADER_H, BLUE_TOP)

    # subtle lower glow strip
    fr(c, 0, H - HEADER_H - 0.6 * mm, W, 1.6 * mm, GLOW_BLUE)

    # ------------------------------------------------------------
    # Doha Bank logo block centered
    # ------------------------------------------------------------

    t(
        c,
        "DOHA BANK",
        W / 2,
        H - HEADER_H + 25 * mm,
        "Caladea-Bold",
        18,
        WHITE,
        "center"
    )

    # ------------------------------------------------------------
    # Main title
    # ------------------------------------------------------------

    t(
        c,
        "Market Updates – Snapshot & Key News",
        W / 2,
        H - HEADER_H + 16 * mm,
        "Caladea-Bold",
        17,
        WHITE,
        "center"
    )

    # ------------------------------------------------------------
    # White divider lines
    # ------------------------------------------------------------

    line_y = H - HEADER_H + 9 * mm

    c.setStrokeColor(WHITE)
    c.setLineWidth(1.1)

    c.line(
        W / 2 - 88 * mm,
        line_y,
        W / 2 - 30 * mm,
        line_y
    )

    c.line(
        W / 2 + 30 * mm,
        line_y,
        W / 2 + 88 * mm,
        line_y
    )

    # ------------------------------------------------------------
    # Date
    # ------------------------------------------------------------

    t(
        c,
        report_date,
        W / 2,
        H - HEADER_H + 6.2 * mm,
        "Carlito-Bold",
        11,
        DATE_BLUE,
        "center"
    )

    # ------------------------------------------------------------
    # Right-side small metadata
    # ------------------------------------------------------------

    meta_x = W - 6 * mm

    t(
        c,
        f"Page {page} of {total}",
        meta_x,
        H - HEADER_H + 31 * mm,
        "Carlito",
        4.5,
        SOFT_WHITE,
        "right"
    )

    t(
        c,
        f"Generated {generated_display_time}",
        meta_x,
        H - HEADER_H + 27 * mm,
        "Carlito",
        4.3,
        SOFT_WHITE,
        "right"
    )

    return H - HEADER_H - 1.2 * mm


def draw_footer(c, report_date):
    fr(c, 0, 0, W, FTR_H, BLUE)
    fr(c, 0, FTR_H - 0.7 * mm, W, 0.7 * mm, CYAN)

    t(c, f"Doha Bank Market Updates – Snapshot & Key News  ·  {report_date}", W - M, 2 * mm, "Carlito", 5.5, WHITE, "right")


# ------------------------------------------------------------
# KPI strip
# ------------------------------------------------------------

def draw_kpi(c, y, kpis):
    if not kpis:
        return y

    cw = UW / len(kpis)

    for i, k in enumerate(kpis):
        val = str(k.get("value", "—"))
        lbl = k.get("label", "")
        sub = k.get("sublabel", "")
        cx = M + i * cw

        fr(c, cx, y - KPI_H, cw, KPI_H, WHITE)
        sr(c, cx, y - KPI_H, cw, KPI_H, RULE_DK, 0.5)
        fr(c, cx, y - 2 * mm, cw, 2 * mm, NAVY)

        vcol = UP if val.startswith("+") else DOWN if val.startswith("-") else NAVY
        t(c, val, cx + 3 * mm, y - 6.5 * mm, "Caladea-Bold", 11, vcol)
        t(c, lbl, cx + 3 * mm, y - 9.5 * mm, "Carlito-Bold", 6.5, TEXT)
        t(c, sub, cx + 3 * mm, y - 13 * mm, "Carlito", 5.5, MUTED)

    return y - KPI_H - 2 * mm


# ------------------------------------------------------------
# Tables
# ------------------------------------------------------------

def sec_hdr(c, x, y, title, w):
    fr(c, x, y - SEC_H, w, SEC_H, BLUE)
    fr(c, x, y - SEC_H, 2 * mm, SEC_H, CYAN)
    t(c, f"| {title}", x + 3 * mm, y - 3.6 * mm, "Caladea-Bold", 7, WHITE)
    return y - SEC_H


def draw_table(c, x, y, hdrs, rows, tw, cws):
    fr(c, x, y - ROW_H, tw, ROW_H, TBLHDR)
    hl(c, x, y - ROW_H, x + tw, CYAN, 0.5)

    cx = x
    for i, (h, cw) in enumerate(zip(hdrs, cws)):
        if i == 0:
            t(c, h, cx + 2 * mm, y - ROW_H + 1.5 * mm, "Carlito-Bold", 6, WHITE)
        else:
            t(c, h, cx + cw / 2, y - ROW_H + 1.5 * mm, "Carlito-Bold", 6, WHITE, "center")
        cx += cw

    y -= ROW_H

    for ri, row in enumerate(rows):
        bg = OFFWHT if ri % 2 == 0 else WHITE
        fr(c, x, y - ROW_H, tw, ROW_H, bg)
        hl(c, x, y - ROW_H, x + tw, RULE, 0.2)

        cx = x
        for ci, (cell, cw) in enumerate(zip(row, cws)):
            cell = safe_text(cell)

            if ci == 0:
                t(c, cell, cx + 2 * mm, y - ROW_H + 1.5 * mm, "Carlito-Bold", 7, TEXT, "left", cw - 3 * mm)
            else:
                col = pct_col(cell)
                fw = "Carlito-Bold" if ("%" in cell or "bps" in cell or cell in ("—", "Pegged", "N/A")) else "Carlito"
                t(c, cell, cx + cw - 1.5 * mm, y - ROW_H + 1.5 * mm, fw, 7, col, "right")

            cx += cw

        y -= ROW_H

    hl(c, x, y, x + tw, RULE_DK, 0.4)
    return y - 1.5 * mm


def section_rows(data, sec):
    out = []

    for r in data.get(sec, []):
        px = r.get("px_last", "N/A")
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


# ------------------------------------------------------------
# News cards
# ------------------------------------------------------------

def draw_news_card(c, x, y, w, h, item, section="global"):
    """
    Clean full-width news card layout.
    Removes:
    - right-side mini highlight boxes
    - vertical blue accent strip
    """

    # Card shadow
    c.setFillColor(colors.HexColor("#DCE7F0"))
    c.roundRect(x + 1.5, y - 1.5, w, h, 6, fill=1, stroke=0)

    # Main card
    c.setFillColor(WHITE)
    c.setStrokeColor(RULE)
    c.setLineWidth(0.6)
    c.roundRect(x, y, w, h, 6, fill=1, stroke=1)

    pad = 8

    content_x = x + pad
    content_y = y + h - pad

    usable_w = w - (pad * 2)

    # Source
    source = item.get("source", "Source")
    ts = item.get("time", "")

    c.setFont("Carlito-Bold", 8)
    c.setFillColor(BLUE)
    c.drawString(content_x, content_y, source)

    # Timestamp
    if ts:
        c.setFont("Carlito", 7)
        c.setFillColor(MUTED)

        ts_w = c.stringWidth(ts, "Carlito", 7)

        c.drawString(
            x + w - pad - ts_w,
            content_y,
            ts
        )

    # Headline
    headline_y = content_y - 13

    c.setFont("Carlito-Bold", 10)
    c.setFillColor(TEXT)

    headline = item.get("title", "")

    headline_lines = split_text(
        headline,
        usable_w,
        "Carlito-Bold",
        10,
        max_lines=2
    )

    yy = headline_y

    for line in headline_lines:
        c.drawString(content_x, yy, line)
        yy -= 11

    # Body
    body = item.get("summary", "") or item.get("description", "")

    body_y = yy - 3

    c.setFont("Carlito", 8)
    c.setFillColor(TEXT)

    body_lines = split_text(
        body,
        usable_w,
        "Carlito",
        8,
        max_lines=5
    )

    yy = body_y

    for line in body_lines:
        c.drawString(content_x, yy, line)
        yy -= 9

    # Bottom separator
    c.setStrokeColor(colors.HexColor("#D8E3EC"))
    c.setLineWidth(0.5)

    c.line(
        x + 6,
        y + 6,
        x + w - 6,
        y + 6
    )

def draw_news_grid(c, x, y, title, items, total_w, rows_count, card_h, card_gap=2 * mm):
    y = sec_hdr(c, x, y, title, total_w)
    y -= 1.5 * mm

    card_w = (total_w - card_gap) / 2
    max_cards = rows_count * 2

    for i, item in enumerate(items[:max_cards]):
        row = i // 2
        col = i % 2
        cx = x + col * (card_w + card_gap)
        cy = y - row * (card_h + card_gap)
        draw_news_card(c, cx, cy, card_w, card_h, item)

    return y - rows_count * (card_h + card_gap)


# ------------------------------------------------------------
# Pages
# ------------------------------------------------------------

def page1(c, report_date, generated_display_time, market_as_of_date, data):
    fr(c, 0, 0, W, H, WHITE)

    top = draw_header(
        c,
        report_date,
        generated_display_time,
        market_as_of_date=market_as_of_date,
        page=1,
        total=2,
        report_status=data.get("report_status", "ok"),
    )

    y = top - 1.5 * mm
    y = draw_kpi(c, y, data.get("kpis", []))

    cw2 = (UW - 4 * mm) / 2
    xL = M
    xR = M + cw2 + 4 * mm

    yL = y
    yL = sec_hdr(c, xL, yL, "GLOBAL INDICES", cw2)
    yL = draw_table(c, xL, yL, ["Market / Index", "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "global_indices"), cw2, cw5(cw2))
    yL -= GAP

    yL = sec_hdr(c, xL, yL, "SPOT CURRENCY", cw2)
    yL = draw_table(c, xL, yL, ["Pair", "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "spot_currency"), cw2, cw5(cw2))
    yL -= GAP

    yL = sec_hdr(c, xL, yL, "QAR CROSS RATES", cw2)
    yL = draw_table(c, xL, yL, ["Pair", "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "qar_cross_rates"), cw2, cw5(cw2))
    yL -= GAP

    yL = sec_hdr(c, xL, yL, "FIXED INCOME - UST YIELDS", cw2)
    yL = draw_table(c, xL, yL, ["Instrument", "Yield", "1D %", "MTD %", "YTD %"], section_rows(data, "fixed_income"), cw2, cw5(cw2))

    yR = y
    yR = sec_hdr(c, xR, yR, "GCC & REGIONAL INDICES", cw2)
    yR = draw_table(c, xR, yR, ["Market / Index", "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "gcc_indices"), cw2, cw5(cw2))
    yR -= GAP

    yR = sec_hdr(c, xR, yR, "QATARI BANKS", cw2)
    yR = draw_table(c, xR, yR, ["Bank", "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "qatari_banks"), cw2, cw5(cw2))
    yR -= GAP

    yR = sec_hdr(c, xR, yR, "COMMODITIES & ENERGY", cw2)
    yR = draw_table(c, xR, yR, ["Asset", "PX Last", "1D %", "MTD %", "YTD %"], section_rows(data, "commodities"), cw2, cw5(cw2))

    draw_footer(c, report_date)


def page2(c, report_date, generated_display_time, market_as_of_date, global_news, qatar_news, report_status="ok"):
    fr(c, 0, 0, W, H, WHITE)

    top = draw_header(
        c,
        report_date,
        generated_display_time,
        market_as_of_date=market_as_of_date,
        page=2,
        total=2,
        report_status=report_status,
    )

    y = top - 2 * mm

    avail = y - FTR_H - 3 * mm
    card_gap = 2 * mm
    between_sections = 3 * mm
    header_space = (2 * SEC_H) + 1.5 * mm + between_sections
    row_space = avail - header_space

    total_rows = 5
    total_internal_gaps = 4 * card_gap
    unit_h = (row_space - total_internal_gaps) / total_rows

    y = draw_news_grid(
        c,
        M,
        y,
        "REGIONAL & GLOBAL NEWS",
        global_news,
        UW,
        rows_count=3,
        card_h=unit_h,
        card_gap=card_gap,
    )

    y -= between_sections

    draw_news_grid(
        c,
        M,
        y,
        "QATAR NEWS",
        qatar_news,
        UW,
        rows_count=2,
        card_h=unit_h,
        card_gap=card_gap,
    )

    draw_footer(c, report_date)


# ------------------------------------------------------------
# Main generator
# ------------------------------------------------------------

def generate(data, output_path):
    report_date = data.get("config", {}).get("report_date", dt.today().strftime("%d %B %Y"))
    generated_display_time = data.get("generated_display_time", "07:00 AST")
    market_as_of_date = data.get("market_as_of_date")
    report_status = data.get("report_status", "PASS")

    c = pdfcanvas.Canvas(output_path, pagesize=landscape(A4))
    c.setTitle(f"Doha Bank Market Updates – Snapshot & Key News - {report_date}")
    c.setAuthor("Doha Bank - AI-generated Daily Briefs")

    page1(c, report_date, generated_display_time, market_as_of_date, data)
    c.showPage()

    page2(
        c,
        report_date,
        generated_display_time,
        market_as_of_date,
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
        out_path = sys.argv[2]

        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        generate(data, out_path)

    else:
        print("Usage: python pdf_generator.py market_data.json report.pdf")
        raise SystemExit(1)

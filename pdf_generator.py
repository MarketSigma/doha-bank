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
            try:
                pdfmetrics.registerFont(TTFont(font_name, path))
            except Exception:
                pass


register_fonts()


def font(name):
    registered = set(pdfmetrics.getRegisteredFontNames())

    fallback = {
        "Caladea": "Times-Roman",
        "Caladea-Bold": "Times-Bold",
        "Caladea-Italic": "Times-Italic",
        "Carlito": "Helvetica",
        "Carlito-Bold": "Helvetica-Bold",
        "Carlito-Italic": "Helvetica-Oblique",
    }

    if name in registered:
        return name

    return fallback.get(name, "Helvetica")


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


def t(c, txt, x, y, font_name="Carlito", size=8, color=TEXT, align="left", maxw=None):
    txt = "" if txt is None else str(txt)
    font_name = font(font_name)

    c.setFont(font_name, size)
    c.setFillColor(color)

    if maxw:
        while len(txt) > 4 and c.stringWidth(txt, font_name, size) > maxw:
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


def ml(c, txt, x, y, font_name, size, color, maxw, lh, maxl=3):
    txt = "" if txt is None else str(txt)
    font_name = font(font_name)

    c.setFont(font_name, size)
    c.setFillColor(color)

    words = txt.split()
    lines = []
    line = ""

    for word in words:
        candidate = (line + "

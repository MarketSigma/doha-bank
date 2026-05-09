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

BLUE = colors.HexColor("#0B4F8A")
NAVY = colors.HexColor("#083B66")
CYAN = colors.HexColor("#00A6D6")
GOLD = colors.HexColor("#D6B45A")
WHITE = colors.white
OFFWHT = colors.HexColor("#F4F8FB")
TBLHDR = colors.HexColor("#0B5E9E")
RULE = colors.HexColor("#B7CAD8")
RULE_DK = colors.HexColor("#8FB3C8")
TEXT = colors.HexColor("#123A5A")
MUTED = colors.HexColor("#6F8594")
UP = colors.HexColor("#008A4B")
DOWN = colors.HexColor("#C0392B")
SUBT = colors.HexColor("#BFD9E8")
WARN = colors.HexColor("#B45309")

# Rest of your original file remains unchanged...

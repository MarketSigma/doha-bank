# ------------------------------------------------------------
# Page constants
# ------------------------------------------------------------

W, H = landscape(A4)
M = 11 * mm
UW = W - 2 * M

# ------------------------------------------------------------
# Modern Institutional Theme
# ------------------------------------------------------------

BLUE = colors.HexColor("#111827")        # Main dark background
NAVY = colors.HexColor("#020617")        # Deep black/navy
CYAN = colors.HexColor("#38BDF8")        # Cyan accent
GOLD = colors.HexColor("#EAB308")        # Gold highlight

WHITE = colors.white
OFFWHT = colors.HexColor("#F8FAFC")      # Soft row background

TBLHDR = colors.HexColor("#1E293B")      # Table header color

RULE = colors.HexColor("#CBD5E1")        # Light border
RULE_DK = colors.HexColor("#94A3B8")     # Strong border

TEXT = colors.HexColor("#0F172A")        # Primary text
MUTED = colors.HexColor("#64748B")       # Secondary text

UP = colors.HexColor("#16A34A")          # Positive values
DOWN = colors.HexColor("#DC2626")        # Negative values

SUBT = colors.HexColor("#CBD5E1")        # Header subtitle
WARN = colors.HexColor("#F59E0B")        # Warning status

HDR_H = 24 * mm
FTR_H = 5.5 * mm
KPI_H = 14 * mm
SEC_H = 5.5 * mm
ROW_H = 4.6 * mm
GAP = 2.5 * mm


# ------------------------------------------------------------
# Header and footer
# ------------------------------------------------------------

def draw_header(c, report_date, generated_display_time, market_as_of_date=None, page=1, total=2, report_status="PASS"):
    fr(c, 0, H - HDR_H, W, HDR_H, BLUE)
    fr(c, 0, H - HDR_H, 56 * mm, HDR_H, NAVY)

    c.setStrokeColor(colors.HexColor("#334155"))
    c.setLineWidth(0.5)
    c.line(56 * mm, H - HDR_H + 3 * mm, 56 * mm, H - 3 * mm)

    c.setFillColor(WHITE)
    c.setStrokeColor(CYAN)
    c.setLineWidth(0.8)
    c.roundRect(5 * mm, H - HDR_H + 5 * mm, 12 * mm, 12 * mm, 1.5 * mm, fill=1, stroke=1)

    t(c, "D", 11 * mm, H - HDR_H + 10 * mm, "Caladea-Bold", 10, BLUE, "center")

    t(c, "بنك الدوحة", 20 * mm, H - HDR_H + 18 * mm, "Carlito", 7, SUBT)
    t(c, "DOHA BANK", 20 * mm, H - HDR_H + 11.5 * mm, "Caladea-Bold", 10, WHITE)

    c.setStrokeColor(GOLD)
    c.setLineWidth(0.8)

    c.line(
        W / 2 - 52 * mm,
        H - HDR_H + 14 * mm,
        W / 2 - 26 * mm,
        H - HDR_H + 14 * mm
    )

    c.line(
        W / 2 + 26 * mm,
        H - HDR_H + 14 * mm,
        W / 2 + 52 * mm,
        H - HDR_H + 14 * mm
    )

    t(
        c,
        "MARKET INTELLIGENCE",
        W / 2,
        H - HDR_H + 19 * mm,
        "Caladea-Bold",
        14,
        WHITE,
        "center"
    )

    t(
        c,
        report_date,
        W / 2,
        H - HDR_H + 12.5 * mm,
        "Carlito",
        9,
        GOLD,
        "center"
    )

    t(
        c,
        "Market Snapshot  |  Currency & Fixed Income  |  Global & Qatar News",
        W / 2,
        H - HDR_H + 7 * mm,
        "Carlito-Italic",
        6.5,
        SUBT,
        "center",
    )

    t(
        c,
        f"Page {page} of {total}",
        W - M,
        H - HDR_H + 19 * mm,
        "Carlito",
        6.5,
        GOLD,
        "right"
    )

    t(
        c,
        f"Generated  {generated_display_time}",
        W - M,
        H - HDR_H + 14 * mm,
        "Carlito",
        6,
        SUBT,
        "right"
    )

    if market_as_of_date:
        t(
            c,
            f"Market data as of {market_as_of_date}",
            W - M,
            H - HDR_H + 9.5 * mm,
            "Carlito",
            5.5,
            SUBT,
            "right"
        )
    else:
        t(
            c,
            "Market data: latest available",
            W - M,
            H - HDR_H + 9.5 * mm,
            "Carlito",
            5.5,
            SUBT,
            "right"
        )

    t(
        c,
        "Supabase  ·  Brave Search  ·  Reuters  ·  Bloomberg",
        W - M,
        H - HDR_H + 5.5 * mm,
        "Carlito",
        5.5,
        SUBT,
        "right"
    )

    status = str(report_status or "PASS").upper()

    if status not in {"PASS", "OK"}:
        t(
            c,
            f"Validation status: {status}",
            W - M,
            H - HDR_H + 2.2 * mm,
            "Carlito-Bold",
            5.5,
            WARN,
            "right"
        )
    else:
        t(
            c,
            "Validation status: PASS",
            W - M,
            H - HDR_H + 2.2 * mm,
            "Carlito-Bold",
            5.5,
            UP,
            "right"
        )

    fr(c, 0, H - HDR_H, W, 1.5 * mm, CYAN)

    return H - HDR_H


def draw_footer(c, report_date):
    fr(c, 0, 0, W, FTR_H, BLUE)
    fr(c, 0, FTR_H - 0.7 * mm, W, 0.7 * mm, CYAN)

    t(
        c,
        "Sources: Supabase market_indices_history  ·  Brave Search  ·  Reuters  ·  Bloomberg  ·  The Peninsula  ·  Qatar Tribune    |    Strictly Confidential - Doha Bank HNWI Clients Only. Not for redistribution.",
        M,
        2 * mm,
        "Carlito-Italic",
        5,
        SUBT,
    )

    t(
        c,
        f"Doha Bank Market Intelligence  ·  {report_date}",
        W - M,
        2 * mm,
        "Carlito",
        5.5,
        WHITE,
        "right"
    )

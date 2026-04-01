"""
Generate test PDFs that mimic real-world LP/GP report formats.
Each PDF has known ground truth for parser validation.

Formats covered:
  A. AQR-style factsheet: summary performance (1M/3M/YTD/1Y), calendar year grid
  B. Ambiguous column headers: "Return" instead of "Net Return"
  C. Gross + Net side by side: two return columns, parser must pick net
  D. Multi-year calendar grid (Winton-style): rows=years, cols=months
     with partial current year (tests trailing-12 logic)
"""
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.units import inch
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "static", "test_pdfs")

# ── Ground truth ────────────────────────────────────────────────────────────

# Shared returns for formats A, B, C
GT_MONTHLY = [1.23, -0.45, 2.10, 0.67, -1.33, 0.89, 1.55, -0.22, 0.98, 1.77, -0.56, 0.44]
GT_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# For format D: 2024 full year + 2025 partial (Jan-Apr)
GT_2024 = [0.95, -0.31, 1.44, 0.82, -1.10, 0.56, 2.01, -0.73, 0.65, 1.88, -0.42, 0.33]
GT_2025 = [1.15, -0.28, 0.77, 1.42]  # only 4 months
# Trailing 12 should be: 2024[May-Dec] + 2025[Jan-Apr]
GT_TRAILING_12 = GT_2024[4:] + GT_2025  # [-1.10, 0.56, 2.01, -0.73, 0.65, 1.88, -0.42, 0.33, 1.15, -0.28, 0.77, 1.42]

# Gross returns = net + ~0.15%/month (simulating 1.8% annual mgmt fee)
GT_GROSS = [round(r + 0.15, 2) for r in GT_MONTHLY]

GROUND_TRUTH = {
    "format_a_aqr_factsheet.pdf": {
        "monthly_returns": GT_MONTHLY,
        "aum_mm": 850.0,
        "fund_name_contains": "Apex",
    },
    "format_b_ambiguous_headers.pdf": {
        "monthly_returns": GT_MONTHLY,
        "aum_mm": 450.41,  # ending NAV = beginning $420M * compound(returns)
        "beginning_nav_mm": 420.0,
        "return_type": "net",  # text says "after all applicable fees" → net
    },
    "format_c_gross_and_net.pdf": {
        "monthly_returns": GT_MONTHLY,  # must pick NET, not gross
        "aum_mm": 1200.0,
        "return_type": "net",
    },
    "format_d_calendar_grid.pdf": {
        "monthly_returns": GT_TRAILING_12,
        "aum_mm": 3200.0,
        "trailing_period": True,  # should show trailing period, not calendar year
    },
    "format_e_parenthetical_negatives.pdf": {
        "monthly_returns": GT_MONTHLY,  # same returns, but negatives use (X.XX) format
        "aum_mm": 579.09,  # ending NAV (beginning $540M * compound returns)
        "beginning_nav_mm": 540.0,
        "return_type": "net",
    },
    "format_f_eur_currency.pdf": {
        "monthly_returns": GT_MONTHLY,
        "aum_mm": 720.0,
        "beginning_nav_mm": 680.0,
        "currency": "EUR",
        "return_type": "net",
    },
}


def _styles():
    s = getSampleStyleSheet()
    return {
        "title": ParagraphStyle("t", parent=s["Heading1"], fontSize=14,
                                textColor=colors.HexColor("#1a3c6e"), spaceAfter=4),
        "sub": ParagraphStyle("s", parent=s["Normal"], fontSize=9,
                              textColor=colors.HexColor("#666"), spaceAfter=2),
        "section": ParagraphStyle("sec", parent=s["Heading2"], fontSize=11,
                                  textColor=colors.HexColor("#1a3c6e"), spaceAfter=6),
        "body": ParagraphStyle("b", parent=s["Normal"], fontSize=9, spaceAfter=4),
        "disc": ParagraphStyle("d", parent=s["Normal"], fontSize=7,
                               textColor=colors.HexColor("#aaa")),
    }


def _table_style_basic():
    return TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), colors.HexColor("#1a3c6e")),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("GRID",          (0, 0), (-1, -1), 0.3, colors.HexColor("#ddd")),
        ("ALIGN",         (1, 0), (-1, -1), "RIGHT"),
    ])


# ── Format A: AQR-style factsheet ──────────────────────────────────────────

def build_format_a():
    """
    Single-page factsheet with:
    - Fund overview (name, AUM, inception, strategy)
    - Summary performance (1M, 3M, YTD, 1Y, SI)
    - Monthly returns calendar grid (single year)
    """
    path = os.path.join(OUTPUT_DIR, "format_a_aqr_factsheet.pdf")
    doc = SimpleDocTemplate(path, pagesize=letter,
                            rightMargin=0.6*inch, leftMargin=0.6*inch,
                            topMargin=0.6*inch, bottomMargin=0.5*inch)
    st = _styles()
    story = []

    story.append(Paragraph("Apex Quantitative Strategies Fund", st["title"]))
    story.append(Paragraph("Monthly Factsheet — December 2025", st["sub"]))
    story.append(Paragraph("Apex Capital Management LLC | UCITS Compliant", st["sub"]))
    story.append(Spacer(1, 0.15*inch))

    # Fund overview
    overview = [
        ["Fund Assets", "$850M", "Strategy", "Systematic Multi-Strategy"],
        ["Inception", "March 2018", "Liquidity", "Monthly"],
        ["Management Fee", "1.5%", "Performance Fee", "15%"],
        ["Currency", "USD", "Benchmark", "HFRI Fund Weighted Composite"],
    ]
    t = Table(overview, colWidths=[1.3*inch, 1.5*inch, 1.3*inch, 2.2*inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#666")),
        ("TEXTCOLOR", (2, 0), (2, -1), colors.HexColor("#666")),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # Summary performance
    story.append(Paragraph("Performance Summary (Net of Fees)", st["section"]))
    ytd = sum(GT_MONTHLY)
    summary = [
        ["", "1 Month", "3 Months", "6 Months", "YTD", "1 Year", "Since Inception"],
        ["Net Return", f"{GT_MONTHLY[-1]:+.2f}%",
         f"{sum(GT_MONTHLY[-3:]):+.2f}%", f"{sum(GT_MONTHLY[-6:]):+.2f}%",
         f"{ytd:+.2f}%", f"{ytd:+.2f}%", "+42.15%"],
        ["Benchmark", "+0.31%", "+0.88%", "+1.92%", "+3.45%", "+3.45%", "+28.70%"],
    ]
    t = Table(summary, colWidths=[1.0*inch, 0.85*inch, 0.85*inch, 0.85*inch,
                                   0.85*inch, 0.85*inch, 1.1*inch])
    t.setStyle(_table_style_basic())
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # Monthly returns grid
    story.append(Paragraph("Monthly Returns (%) — 2025", st["section"]))
    header = [""] + GT_MONTHS + ["YTD"]
    row = ["2025"] + [f"{r:+.2f}" for r in GT_MONTHLY] + [f"{ytd:+.2f}"]
    grid = [header, row]
    t = Table(grid, colWidths=[0.5*inch] + [0.48*inch]*12 + [0.56*inch])
    t.setStyle(_table_style_basic())
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # Disclaimer
    story.append(Paragraph(
        "Past performance is not indicative of future results. Returns are net of all fees. "
        "This document is for informational purposes only.", st["disc"]))

    doc.build(story)
    print(f"  ✓ Format A (AQR factsheet): {path}")


# ── Format B: Ambiguous column headers ──────────────────────────────────────

def build_format_b():
    """
    LP quarterly report with ambiguous header: "Return" instead of "Net Return".
    Parser should flag return_type as "unknown" and lower confidence.
    """
    path = os.path.join(OUTPUT_DIR, "format_b_ambiguous_headers.pdf")
    doc = SimpleDocTemplate(path, pagesize=letter,
                            rightMargin=0.75*inch, leftMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.6*inch)
    st = _styles()
    story = []

    story.append(Paragraph("Granite Peak Multi-Strategy Fund LP", st["title"]))
    story.append(Paragraph("Annual Performance Report — FY 2025", st["sub"]))
    story.append(Spacer(1, 0.15*inch))

    # Capital account — ending NAV must be consistent with monthly returns
    beginning_nav = 420_000_000
    compound = 1.0
    for r in GT_MONTHLY:
        compound *= (1 + r / 100)
    ending_nav = round(beginning_nav * compound)
    story.append(Paragraph("Capital Account Summary", st["section"]))
    cap = [
        ["Beginning NAV (Jan 1, 2025)", f"${beginning_nav:,.0f}"],
        ["Ending NAV (Dec 31, 2025)", f"${ending_nav:,.0f}"],
    ]
    t = Table(cap, colWidths=[4*inch, 2.5*inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # AMBIGUOUS header — "Return" not "Net Return"
    story.append(Paragraph("Monthly Performance", st["section"]))
    header = ["Month", "Return (%)", "Cumulative"]
    rows = [header]
    cum = 100.0
    for i, r in enumerate(GT_MONTHLY):
        cum *= (1 + r / 100)
        month_name = f"{GT_MONTHS[i]} 2025"
        rows.append([month_name, f"{r:+.2f}%", f"{cum:.2f}"])
    t = Table(rows, colWidths=[2*inch, 1.5*inch, 1.5*inch])
    t.setStyle(_table_style_basic())
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    story.append(Paragraph("Returns are stated after all applicable fees and expenses.", st["disc"]))
    doc.build(story)
    print(f"  ✓ Format B (ambiguous headers): {path}")


# ── Format C: Gross + Net side by side ──────────────────────────────────────

def build_format_c():
    """
    Report with BOTH gross and net return columns.
    Parser must pick "Net Return", not "Gross Return".
    """
    path = os.path.join(OUTPUT_DIR, "format_c_gross_and_net.pdf")
    doc = SimpleDocTemplate(path, pagesize=letter,
                            rightMargin=0.75*inch, leftMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.6*inch)
    st = _styles()
    story = []

    story.append(Paragraph("Summit Credit Opportunities Fund LP", st["title"]))
    story.append(Paragraph("Monthly Performance Report — 2025", st["sub"]))
    story.append(Spacer(1, 0.15*inch))

    story.append(Paragraph("Fund Overview", st["section"]))
    overview = [
        ["Fund AUM", "$1.2B", "Strategy", "Credit Long/Short"],
        ["Management Fee", "2%", "Incentive Fee", "20%"],
    ]
    t = Table(overview, colWidths=[1.3*inch, 1.5*inch, 1.3*inch, 2.2*inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # TWO return columns: Gross Return FIRST, then Net Return
    story.append(Paragraph("Monthly Performance Attribution", st["section"]))
    header = ["Month", "Gross Return (%)", "Net Return (%)", "S&P 500"]
    rows = [header]
    for i, (net, gross) in enumerate(zip(GT_MONTHLY, GT_GROSS)):
        month_name = f"{GT_MONTHS[i]} 2025"
        spy = round(net * 0.7 + 0.1, 2)  # fake benchmark
        rows.append([month_name, f"{gross:+.2f}%", f"{net:+.2f}%", f"{spy:+.2f}%"])
    t = Table(rows, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1.3*inch])
    t.setStyle(_table_style_basic())
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    story.append(Paragraph(
        "Gross returns are before management fees and incentive allocation. "
        "Net returns are after all fees. Past performance is not indicative of future results.",
        st["disc"]))
    doc.build(story)
    print(f"  ✓ Format C (gross + net columns): {path}")


# ── Format D: Multi-year calendar grid (Winton-style) ──────────────────────

def build_format_d():
    """
    Calendar grid with rows=years, cols=months.
    2024 has 12 months, 2025 has only 4 months (Jan-Apr).
    Parser should return trailing 12 (May 2024 – Apr 2025), NOT calendar 2024.
    """
    path = os.path.join(OUTPUT_DIR, "format_d_calendar_grid.pdf")
    doc = SimpleDocTemplate(path, pagesize=letter,
                            rightMargin=0.5*inch, leftMargin=0.5*inch,
                            topMargin=0.6*inch, bottomMargin=0.5*inch)
    st = _styles()
    story = []

    story.append(Paragraph("Vanguard Systematic Macro UCITS Fund", st["title"]))
    story.append(Paragraph("Monthly Performance Report — April 2025", st["sub"]))
    story.append(Paragraph("Vanguard Alternative Investments Ltd", st["sub"]))
    story.append(Spacer(1, 0.15*inch))

    # Fund info
    overview = [
        ["Fund Assets", "$3.2bn", "Strategy", "Systematic Macro / CTA"],
        ["Inception", "January 2019", "Share Class", "USD Institutional"],
        ["Management Fee", "1.0%", "Performance Fee", "10% (HWM)"],
    ]
    t = Table(overview, colWidths=[1.3*inch, 1.5*inch, 1.3*inch, 2.5*inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # Calendar grid — THIS IS THE KEY FORMAT
    story.append(Paragraph("Net Performance (%) — Calendar Year Returns", st["section"]))
    header = [""] + GT_MONTHS + ["YTD"]
    row_2024 = ["2024"] + [f"{r:+.2f}" for r in GT_2024] + [f"{sum(GT_2024):+.2f}"]
    row_2025 = ["2025"] + [f"{r:+.2f}" for r in GT_2025] + ["-"] * (12 - len(GT_2025)) + [f"{sum(GT_2025):+.2f}"]
    grid = [header, row_2024, row_2025]
    t = Table(grid, colWidths=[0.5*inch] + [0.48*inch]*12 + [0.56*inch])
    t.setStyle(_table_style_basic())
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    story.append(Paragraph(
        "Performance is net of all fees and expenses. Past performance is not "
        "indicative of future results. Returns for periods less than one year are not annualized.",
        st["disc"]))
    doc.build(story)
    print(f"  ✓ Format D (calendar grid, partial year): {path}")


# ── Format E: Parenthetical negatives (US GAAP / fund admin style) ────────

def build_format_e():
    """
    LP capital account statement using parenthetical negatives: (0.91)% instead of -0.91%.
    This is standard US GAAP format used by fund admins (Citco, SS&C, NAV Consulting).
    Parser must correctly interpret (X.XX) as negative, not positive.
    """
    path = os.path.join(OUTPUT_DIR, "format_e_parenthetical_negatives.pdf")
    doc = SimpleDocTemplate(path, pagesize=letter,
                            rightMargin=0.75*inch, leftMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.6*inch)
    st = _styles()
    story = []

    story.append(Paragraph("Blackstone Credit Strategies Fund LP", st["title"]))
    story.append(Paragraph("Capital Account Statement — FY 2025", st["sub"]))
    story.append(Paragraph("Prepared by SS&C Fund Administration", st["sub"]))
    story.append(Spacer(1, 0.15*inch))

    # Capital account
    beginning_nav = 540_000_000
    compound = 1.0
    for r in GT_MONTHLY:
        compound *= (1 + r / 100)
    ending_nav = round(beginning_nav * compound)
    story.append(Paragraph("Capital Account Summary", st["section"]))
    cap = [
        ["Beginning NAV (Jan 1, 2025)", f"${beginning_nav:,.0f}"],
        ["Ending NAV (Dec 31, 2025)", f"${ending_nav:,.0f}"],
        ["Net Assets Under Management", "$580M"],
    ]
    t = Table(cap, colWidths=[4*inch, 2.5*inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # Monthly returns with PARENTHETICAL NEGATIVES — this is the key format
    story.append(Paragraph("Monthly Net Return (%)", st["section"]))
    header = ["Month", "Net Return (%)", "Cumulative NAV"]
    rows = [header]
    cum = 100.0
    for i, r in enumerate(GT_MONTHLY):
        cum *= (1 + r / 100)
        month_name = f"{GT_MONTHS[i]} 2025"
        # Format negative returns with parentheses: (0.91)% instead of -0.91%
        if r < 0:
            ret_str = f"({abs(r):.2f})%"
        else:
            ret_str = f"{r:.2f}%"
        rows.append([month_name, ret_str, f"{cum:.2f}"])
    t = Table(rows, colWidths=[2*inch, 1.5*inch, 1.5*inch])
    t.setStyle(_table_style_basic())
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    story.append(Paragraph(
        "Returns are net of all management fees and incentive allocations. "
        "Negative returns are shown in parentheses per US GAAP convention. "
        "Management Fee: 1.5% annual. Incentive Fee: 15%.",
        st["disc"]))
    doc.build(story)
    print(f"  ✓ Format E (parenthetical negatives): {path}")


# ── Format F: EUR-denominated fund (European currency) ──────────────────────

def build_format_f():
    """
    European fund using EUR currency throughout.
    Tests multi-currency parsing: € symbol, EUR suffix, and European number format.
    Parser must detect currency as EUR and correctly parse €-denominated amounts.
    """
    path = os.path.join(OUTPUT_DIR, "format_f_eur_currency.pdf")
    doc = SimpleDocTemplate(path, pagesize=letter,
                            rightMargin=0.75*inch, leftMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.6*inch)
    st = _styles()
    story = []

    story.append(Paragraph("Amundi European Credit Opportunities Fund", st["title"]))
    story.append(Paragraph("Quarterly Report — Q4 2025", st["sub"]))
    story.append(Paragraph("Amundi Asset Management | Paris, France", st["sub"]))
    story.append(Spacer(1, 0.15*inch))

    # Fund overview with EUR
    story.append(Paragraph("Fund Overview", st["section"]))
    overview = [
        ["Fund Size", "€720,000,000", "Strategy", "European Credit L/S"],
        ["Share Class", "EUR Institutional", "Liquidity", "Monthly"],
        ["Management Fee", "1.25%", "Incentive Fee", "12.5%"],
        ["Currency", "EUR", "Benchmark", "Euro STOXX 50"],
    ]
    t = Table(overview, colWidths=[1.3*inch, 1.5*inch, 1.3*inch, 2.2*inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#666")),
        ("TEXTCOLOR", (2, 0), (2, -1), colors.HexColor("#666")),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # Capital account in EUR
    beginning_nav = 680_000_000
    compound = 1.0
    for r in GT_MONTHLY:
        compound *= (1 + r / 100)
    ending_nav = round(beginning_nav * compound)
    story.append(Paragraph("Capital Account Summary", st["section"]))
    cap = [
        ["Beginning NAV (Jan 1, 2025)", f"€{beginning_nav:,.0f}"],
        ["Ending NAV (Dec 31, 2025)", f"€{ending_nav:,.0f}"],
        ["Total Net Assets", "€720,000,000"],
    ]
    t = Table(cap, colWidths=[4*inch, 2.5*inch])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    # Monthly returns table
    story.append(Paragraph("Monthly Net Performance (%)", st["section"]))
    header = ["Month", "Net Return (%)", "Cumulative"]
    rows = [header]
    cum = 100.0
    for i, r in enumerate(GT_MONTHLY):
        cum *= (1 + r / 100)
        month_name = f"{GT_MONTHS[i]} 2025"
        rows.append([month_name, f"{r:+.2f}%", f"{cum:.2f}"])
    t = Table(rows, colWidths=[2*inch, 1.5*inch, 1.5*inch])
    t.setStyle(_table_style_basic())
    story.append(t)
    story.append(Spacer(1, 0.2*inch))

    story.append(Paragraph(
        "All values denominated in EUR. Performance is net of all management fees "
        "and incentive allocations. Past performance is not indicative of future results.",
        st["disc"]))
    doc.build(story)
    print(f"  ✓ Format F (EUR currency): {path}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Generating test PDFs...")
    build_format_a()
    build_format_b()
    build_format_c()
    build_format_d()
    build_format_e()
    build_format_f()
    print(f"\nGround truth for validation:")
    for name, gt in GROUND_TRUTH.items():
        print(f"  {name}: {len(gt['monthly_returns'])} months, AUM={gt.get('aum_mm')}")
    print(f"\nAll test PDFs in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()

"""
Generate a realistic LP quarterly report PDF for demo purposes.
Run once: python pipeline/generate_sample_pdf.py
"""
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.units import inch
import os

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "static", "sample_lp_report.pdf")

FUND_NAME    = "Meridian Global Macro Fund LP"
MANAGER      = "Meridian Capital Management LLC"
PERIOD       = "January 2025 – December 2025"
INVESTOR     = "Equi Alternatives Fund I"
ACCOUNT_NO   = "EQ-2024-0047"

# Monthly returns (%) — realistic macro fund
MONTHLY_RETURNS = [
    ("January 2025",   "+1.82"),
    ("February 2025",  "+0.54"),
    ("March 2025",     "-0.91"),
    ("April 2025",     "+2.13"),
    ("May 2025",       "+0.38"),
    ("June 2025",      "-1.44"),
    ("July 2025",      "+3.07"),
    ("August 2025",    "+1.21"),
    ("September 2025", "-0.67"),
    ("October 2025",   "+1.95"),
    ("November 2025",  "+0.83"),
    ("December 2025",  "+1.42"),
]

def build_pdf():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    doc = SimpleDocTemplate(OUTPUT_PATH, pagesize=letter,
                            rightMargin=inch*0.75, leftMargin=inch*0.75,
                            topMargin=inch*0.75, bottomMargin=inch*0.75)
    styles = getSampleStyleSheet()
    story  = []

    # ── Header ──────────────────────────────────────────────────────────────
    title_style = ParagraphStyle("title", parent=styles["Heading1"],
                                 fontSize=16, spaceAfter=4, textColor=colors.HexColor("#0d2b4e"))
    sub_style   = ParagraphStyle("sub", parent=styles["Normal"],
                                 fontSize=10, textColor=colors.HexColor("#555555"), spaceAfter=2)
    label_style = ParagraphStyle("label", parent=styles["Normal"],
                                 fontSize=9, textColor=colors.HexColor("#888888"))
    body_style  = ParagraphStyle("body", parent=styles["Normal"],
                                 fontSize=10, spaceAfter=6)

    story.append(Paragraph(FUND_NAME, title_style))
    story.append(Paragraph(f"Managed by {MANAGER}", sub_style))
    story.append(Paragraph("Capital Account Statement — Annual Performance Report", sub_style))
    story.append(Spacer(1, 0.15*inch))

    # ── Account info table ───────────────────────────────────────────────────
    info_data = [
        ["Investor Name:",  INVESTOR,   "Reporting Period:", PERIOD],
        ["Account Number:", ACCOUNT_NO, "Fund Strategy:",    "Global Macro / CTA"],
        ["Currency:",       "USD",      "Liquidity:",        "Quarterly (90-day notice)"],
    ]
    info_table = Table(info_data, colWidths=[1.4*inch, 2.2*inch, 1.4*inch, 2.2*inch])
    info_table.setStyle(TableStyle([
        ("FONTSIZE",    (0,0), (-1,-1), 9),
        ("TEXTCOLOR",   (0,0), (0,-1), colors.HexColor("#555555")),
        ("TEXTCOLOR",   (2,0), (2,-1), colors.HexColor("#555555")),
        ("FONTNAME",    (0,0), (0,-1), "Helvetica-Bold"),
        ("FONTNAME",    (2,0), (2,-1), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING",    (0,0), (-1,-1), 4),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 0.25*inch))

    # ── Capital account summary ──────────────────────────────────────────────
    story.append(Paragraph("Capital Account Summary", ParagraphStyle("sec",
        parent=styles["Heading2"], fontSize=12, textColor=colors.HexColor("#0d2b4e"), spaceAfter=8)))

    cap_data = [
        ["Beginning NAV (Jan 1, 2025)",      "$2,450,000.00"],
        ["Capital Contributions",            "$0.00"],
        ["Capital Withdrawals",              "$0.00"],
        ["Net Performance (Jan–Dec 2025)",   "+$260,871.00"],
        ["Management Fees (1% annual)",      "($24,500.00)"],
        ["Incentive Allocation (10%)",        "($26,087.00)"],
        ["Ending NAV (Dec 31, 2025)",        "$2,660,284.00"],
    ]
    cap_table = Table(cap_data, colWidths=[4.5*inch, 2.5*inch])
    cap_table.setStyle(TableStyle([
        ("FONTSIZE",      (0,0), (-1,-1), 10),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("TOPPADDING",    (0,0), (-1,-1), 5),
        ("LINEBELOW",     (0,-2), (-1,-2), 0.5, colors.HexColor("#cccccc")),
        ("FONTNAME",      (0,-1), (-1,-1), "Helvetica-Bold"),
        ("TEXTCOLOR",     (1,3),  (1,3),   colors.HexColor("#1a7a3c")),
        ("TEXTCOLOR",     (0,-1), (-1,-1), colors.HexColor("#0d2b4e")),
        ("LINEABOVE",     (0,-1), (-1,-1), 1.0, colors.HexColor("#0d2b4e")),
    ]))
    story.append(cap_table)
    story.append(Spacer(1, 0.25*inch))

    # ── Monthly performance table ────────────────────────────────────────────
    story.append(Paragraph("Monthly Performance Attribution",
        ParagraphStyle("sec", parent=styles["Heading2"], fontSize=12,
                       textColor=colors.HexColor("#0d2b4e"), spaceAfter=8)))

    perf_header = [["Month", "Net Return (%)", "Cumulative NAV", "Notes"]]
    cum_nav = 2450000.0
    perf_rows = []
    for month, ret in MONTHLY_RETURNS:
        cum_nav *= (1 + float(ret.replace("+","").replace("%",""))/100)
        color_hint = "Positive month" if float(ret.replace("+","")) >= 0 else "Negative month"
        perf_rows.append([month, ret + "%", f"${cum_nav:,.0f}", color_hint])

    perf_data  = perf_header + perf_rows
    perf_table = Table(perf_data, colWidths=[2.0*inch, 1.4*inch, 1.8*inch, 2.0*inch])
    perf_style = [
        ("BACKGROUND",    (0,0), (-1,0),  colors.HexColor("#0d2b4e")),
        ("TEXTCOLOR",     (0,0), (-1,0),  colors.white),
        ("FONTNAME",      (0,0), (-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0), (-1,-1), 9),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("TOPPADDING",    (0,0), (-1,-1), 5),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [colors.HexColor("#f7f9fc"), colors.white]),
        ("GRID",          (0,0), (-1,-1), 0.3, colors.HexColor("#dddddd")),
        ("ALIGN",         (1,0), (2,-1),  "RIGHT"),
    ]
    # Colour negative months red
    for i, (_, ret) in enumerate(MONTHLY_RETURNS, start=1):
        if float(ret.replace("+","")) < 0:
            perf_style.append(("TEXTCOLOR", (1,i), (1,i), colors.HexColor("#c0392b")))
        else:
            perf_style.append(("TEXTCOLOR", (1,i), (1,i), colors.HexColor("#1a7a3c")))
    perf_table.setStyle(TableStyle(perf_style))
    story.append(perf_table)
    story.append(Spacer(1, 0.25*inch))

    # ── Risk metrics ─────────────────────────────────────────────────────────
    story.append(Paragraph("Risk & Performance Metrics (Full Year 2025)",
        ParagraphStyle("sec", parent=styles["Heading2"], fontSize=12,
                       textColor=colors.HexColor("#0d2b4e"), spaceAfter=8)))

    risk_data = [
        ["Annualized Return (net of fees)", "+10.64%"],
        ["Annualized Volatility",           "6.82%"],
        ["Sharpe Ratio (Rf = 5.0%)",        "0.82"],
        ["Maximum Drawdown",                "-2.17%"],
        ["Sortino Ratio",                   "1.44"],
        ["Correlation to S&P 500 (SPY)",    "-0.12"],
        ["Beta to S&P 500",                 "0.04"],
    ]
    risk_table = Table(risk_data, colWidths=[4.5*inch, 2.5*inch])
    risk_table.setStyle(TableStyle([
        ("FONTSIZE",      (0,0), (-1,-1), 10),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("TOPPADDING",    (0,0), (-1,-1), 5),
        ("ROWBACKGROUNDS",(0,0), (-1,-1), [colors.HexColor("#f7f9fc"), colors.white]),
        ("GRID",          (0,0), (-1,-1), 0.3, colors.HexColor("#dddddd")),
        ("ALIGN",         (1,0), (1,-1),  "RIGHT"),
    ]))
    story.append(risk_table)
    story.append(Spacer(1, 0.3*inch))

    # ── Footer disclaimer ────────────────────────────────────────────────────
    disc = ("This report is prepared for informational purposes only and is intended solely for "
            "the named investor. Past performance is not indicative of future results. "
            "Net returns are stated after management fees and incentive allocation. "
            "All figures are unaudited preliminary estimates subject to revision.")
    story.append(Paragraph(disc, ParagraphStyle("disc", parent=styles["Normal"],
                                                fontSize=7.5, textColor=colors.HexColor("#aaaaaa"))))

    doc.build(story)
    print(f"✓ PDF generated: {OUTPUT_PATH}")

if __name__ == "__main__":
    build_pdf()

"""
PDF Ingest — extract monthly NAV returns from LP quarterly/annual reports.

Handles the most common LP statement format:
  - Table with columns: Month | Net Return (%) | ...
  - Searches all pages for a performance table
  - Returns a normalized fund dict matching the standard ingest schema

Usage:
    from pipeline.ingest_pdf import load_fund_from_pdf
    fund = load_fund_from_pdf("static/sample_lp_report.pdf")
"""

import re
import os
import pdfplumber

# ── Regex patterns ────────────────────────────────────────────────────────────
_RETURN_PATTERN = re.compile(
    r"([+-]?\d{1,3}\.\d{1,4})\s*%?"   # e.g. +1.82% or -0.91
)

_MONTH_KEYWORDS = {
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
    "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
}

_FUND_NAME_LABELS = ("fund name", "fund:", "managed by", "fund lp", "capital fund", "macro fund")
_AUM_LABELS       = ("ending nav", "ending capital", "total nav", "fund aum")
_PERIOD_LABELS    = ("reporting period", "period:", "for the period")


def _extract_text_and_tables(path: str):
    """Return (full_text, list_of_tables) from all pages."""
    full_text = []
    all_tables = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            full_text.append(page.extract_text() or "")
            tables = page.extract_tables()
            if tables:
                all_tables.extend(tables)
    return "\n".join(full_text), all_tables


def _find_fund_name(text: str) -> str:
    """Extract fund name from header text."""
    for line in text.splitlines():
        line_l = line.lower()
        if any(kw in line_l for kw in ("fund lp", "fund, lp", "capital lp", "macro fund", "credit fund")):
            return line.strip()
        if "fund" in line_l and len(line.split()) <= 8 and len(line) > 8:
            return line.strip()
    return "Unknown Fund"


def _find_aum(text: str):
    """Extract ending NAV / AUM from text — requires explicit $ sign to avoid date false positives."""
    for line in text.splitlines():
        line_l = line.lower()
        if any(lbl in line_l for lbl in _AUM_LABELS):
            # Must have an explicit $ to distinguish from dates like "Dec 31"
            m = re.search(r"\$([\d,]+(?:\.\d{2})?)", line)
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    if val > 100_000:           # sanity: real NAV is > $100K
                        return round(val / 1_000_000, 2)
                except ValueError:
                    pass
    return None


def _parse_return_from_row(cells):
    """Extract a monthly return float from a table row's cells. Returns float or None."""
    for cell in cells[1:]:
        clean = cell.replace("%", "").replace("+", "").strip()
        try:
            val = float(clean)
            if -20 <= val <= 20:
                return val / 100.0
        except ValueError:
            continue
    return None


def _extract_monthly_returns_from_tables(tables: list):
    """
    Find the performance table across all tables and extract monthly returns.
    Handles the case where pdfplumber splits the table across pages into multiple tables.
    Returns list of floats (decimal) e.g. [0.0182, -0.0091, ...]
    """
    all_returns = []

    for table in tables:
        table_returns = []
        has_month_col = False

        for row in table:
            if not row:
                continue
            cells = [str(c or "").strip() for c in row]
            row_text = " ".join(cells).lower()

            # Detect header row — marks this table as a performance table
            if any(kw in row_text for kw in ("net return", "monthly return", "performance", "return (%)")):
                has_month_col = True
                continue

            first_cell = cells[0].lower() if cells else ""
            is_month_row = any(kw in first_cell for kw in _MONTH_KEYWORDS)

            if is_month_row or has_month_col:
                val = _parse_return_from_row(cells)
                if val is not None:
                    table_returns.append(val)

        # Accept this table if it has 6+ months (main table) or 1-5 months (continuation)
        if len(table_returns) >= 6:
            all_returns = table_returns   # main performance table found
        elif 1 <= len(table_returns) <= 5 and all_returns:
            all_returns.extend(table_returns)  # continuation rows (split across pages)

    return all_returns


def _extract_monthly_returns_from_text(text: str):
    """
    Fallback: scan raw text for month-return pairs when table extraction fails.
    """
    returns = []
    lines = text.splitlines()

    for line in lines:
        line_l = line.lower()
        if any(kw in line_l for kw in _MONTH_KEYWORDS):
            m = _RETURN_PATTERN.search(line)
            if m:
                try:
                    val = float(m.group(1))
                    if -20 <= val <= 20:
                        returns.append(val / 100.0)
                except ValueError:
                    pass

    return returns


def load_fund_from_pdf(path: str) -> dict:
    """
    Main entry point. Parse an LP report PDF and return a normalized fund dict.

    Returns:
        {
            "ticker":        str,
            "name":          str,
            "aum_mm":        float | None,
            "raw_returns":   list[float],   # 12 monthly returns
            "source_format": "pdf",
            "source_path":   str,
            "extraction": {
                "pages_scanned": int,
                "tables_found":  int,
                "method":        "table" | "text" | "failed",
                "returns_count": int,
            }
        }

    Raises:
        ValueError if fewer than 3 months of returns found.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"PDF not found: {path}")

    text, tables = _extract_text_and_tables(path)

    # Count pages
    with pdfplumber.open(path) as pdf:
        pages_scanned = len(pdf.pages)

    fund_name = _find_fund_name(text)
    aum_mm    = _find_aum(text)

    # Try table extraction first, fall back to text scan
    returns = _extract_monthly_returns_from_tables(tables)
    method  = "table"
    if len(returns) < 3:
        returns = _extract_monthly_returns_from_text(text)
        method  = "text"
    if len(returns) < 3:
        method = "failed"

    # Take last 12 months if more than 12 found
    returns = returns[-12:]

    if len(returns) < 3:
        raise ValueError(
            f"Insufficient return data extracted from {path}: "
            f"found {len(returns)} months (need ≥ 3)"
        )

    # Derive ticker-like id from file name
    ticker = os.path.splitext(os.path.basename(path))[0].upper()[:8]

    return {
        "fund_id":       ticker,
        "ticker":        ticker,
        "name":          fund_name,
        "aum_mm":        aum_mm,
        "raw_returns":   returns,
        "source_format": "pdf",
        "source_path":   path,
        "extraction": {
            "pages_scanned": pages_scanned,
            "tables_found":  len(tables),
            "method":        method,
            "returns_count": len(returns),
        },
    }


# ── CLI test ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json, sys
    path = sys.argv[1] if len(sys.argv) > 1 else "static/sample_lp_report.pdf"
    result = load_fund_from_pdf(path)
    ext = result.pop("extraction")
    print(json.dumps(result, indent=2))
    print(f"\nExtraction metadata: {ext}")

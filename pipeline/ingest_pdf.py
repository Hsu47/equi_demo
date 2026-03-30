"""
PDF Ingest — extract monthly NAV returns from LP quarterly/annual reports.

Handles multiple common LP statement formats:
  - Vertical table:  Month | Net Return (%) | ...
  - Horizontal/calendar table: rows=months, cols=years
  - Date variants: "January 2025", "Jan 2025", "2025-01", "01/2025"
  - Summary/factsheet format: extract available period returns (1-Mth, YTD, etc.)
  - Fallback to raw text scanning when table extraction fails

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

# ISO / numeric date patterns:  2025-01, 01/2025, 1/2025
_DATE_PATTERN = re.compile(
    r"(\d{4})[-/](\d{1,2})"   # 2025-01
    r"|(\d{1,2})/(\d{4})"     # 01/2025
)

_FUND_NAME_LABELS = ("fund name", "fund:", "managed by", "fund lp", "capital fund", "macro fund")
_AUM_LABELS       = ("ending nav", "ending capital", "total nav", "fund aum")
_PERIOD_LABELS    = ("reporting period", "period:", "for the period")

# Expanded header detection — covers most LP report variants
_RETURN_HEADER_KEYWORDS = {
    "net return", "monthly return", "performance", "return (%)",
    "net ror", "monthly p&l", "return (net)", "net perf",
    "nav change", "net of fees", "return(%)", "net ret",
    "monthly ror", "period return", "monthly net",
    "gross return", "net return", "calendar year",
    "monthly returns", "net performance",
}

# Cells that represent missing data (not errors)
_SKIP_CELLS = {"", "-", "—", "–", "n/a", "na", "null", "none", "*", "—-"}

# Summary/factsheet period labels (for non-LP-report PDFs)
_SUMMARY_PERIOD_LABELS = {
    "1-mth", "1 mth", "1 month", "1-month", "1m",
    "3-mth", "3 mth", "3 month", "3-month", "3m",
    "6-mth", "6 mth", "6 month", "6-month", "6m",
    "ytd", "1-yr", "1 yr", "1 year", "1-year",
    "3-yr", "3 yr", "5-yr", "5 yr", "10-yr", "10 yr",
    "since inception",
}


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
        # Skip lines that are clearly not fund names
        if any(skip in line_l for skip in ("inception", "expense", "cusip", "listing")):
            continue
        if any(kw in line_l for kw in ("fund lp", "fund, lp", "capital lp", "macro fund", "credit fund")):
            return line.strip()
    # Second pass: look for "ETF" or "Fund" in short lines
    for line in text.splitlines():
        line_l = line.lower()
        if any(skip in line_l for skip in ("inception", "expense", "cusip", "listing", "return")):
            continue
        if ("etf" in line_l or "fund" in line_l) and len(line.split()) <= 8 and len(line) > 8:
            return line.strip()
    return "Unknown Fund"


def _derive_ticker(fund_name: str, file_path: str) -> str:
    """
    Generate a meaningful ticker from the fund name.
    E.g. "Meridian Global Macro Fund LP" → "MGMF"
    Falls back to filename-based ticker if name is unknown.
    """
    if fund_name and fund_name != "Unknown Fund":
        # Take first letter of each significant word (skip articles, suffixes)
        skip = {"lp", "llc", "inc", "ltd", "the", "a", "an", "of", "and", "fund", "capital"}
        words = fund_name.split()
        initials = [w[0].upper() for w in words
                    if w.lower() not in skip and len(w) > 1]
        if len(initials) >= 2:
            return "".join(initials)[:6]
    # Fallback: filename
    return os.path.splitext(os.path.basename(file_path))[0].upper()[:8]


def _find_aum(text: str):
    """
    Extract ending NAV / AUM from text.
    Handles:
      - "$2,660,284.00"  (standard)
      - "$2.66M" / "$2.66 million"  (abbreviated)
      - "$2.66B" / "$2.66 billion"  (abbreviated)
      - "2,660,284 USD"  (no $ prefix, USD suffix)
      - "NAV: 2,660,284"  (bare number on same or next line)
      - Line-continuation where $ amount is on the next line
    """
    lines = text.splitlines()

    def _try_parse_amount(search_text):
        """Try multiple AUM formats on a text string. Returns value in millions or None."""
        # Format 1: $X.XXM / $X.XX million
        m = re.search(r"\$([\d,.]+)\s*[Mm](?:illion)?", search_text)
        if m:
            try:
                return round(float(m.group(1).replace(",", "")), 2)
            except ValueError:
                pass
        # Format 2: $X.XXB / $X.XX billion
        m = re.search(r"\$([\d,.]+)\s*[Bb](?:illion)?", search_text)
        if m:
            try:
                return round(float(m.group(1).replace(",", "")) * 1000, 2)
            except ValueError:
                pass
        # Format 3: $X,XXX,XXX.XX (standard dollar amount)
        m = re.search(r"\$([\d,]+(?:\.\d{1,2})?)", search_text)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val > 100_000:
                    return round(val / 1_000_000, 2)
            except ValueError:
                pass
        # Format 4: X,XXX,XXX USD
        m = re.search(r"([\d,]+(?:\.\d{1,2})?)\s*USD", search_text)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val > 100_000:
                    return round(val / 1_000_000, 2)
            except ValueError:
                pass
        # Format 5: bare large number (no currency symbol)
        m = re.search(r"(?<!\d)([\d,]{7,}(?:\.\d{1,2})?)(?!\d)", search_text)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val > 100_000:
                    return round(val / 1_000_000, 2)
            except ValueError:
                pass
        return None

    for i, line in enumerate(lines):
        line_l = line.lower()
        if any(lbl in line_l for lbl in _AUM_LABELS):
            # Try current line
            result = _try_parse_amount(line)
            if result is not None:
                return result
            # Try next line (line-continuation from pdfplumber)
            if i + 1 < len(lines):
                result = _try_parse_amount(lines[i + 1])
                if result is not None:
                    return result
            # Try: label line ends with $ and amount is at start of next line
            if line.rstrip().endswith("$") and i + 1 < len(lines):
                m = re.search(r"^([\d,]+(?:\.\d{2})?)", lines[i + 1].strip())
                if m:
                    try:
                        val = float(m.group(1).replace(",", ""))
                        if val > 100_000:
                            return round(val / 1_000_000, 2)
                    except ValueError:
                        pass
    return None


def _is_month_cell(cell_text: str) -> bool:
    """Check if a cell contains a month reference (name or ISO date)."""
    cell_l = cell_text.lower().strip()
    if any(kw in cell_l for kw in _MONTH_KEYWORDS):
        return True
    if _DATE_PATTERN.search(cell_text):
        return True
    return False


def _normalize_cell(cell) -> str:
    """Normalize a table cell value for return parsing."""
    if cell is None:
        return ""
    s = str(cell).strip()
    # Remove percentage signs, plus signs, whitespace variants
    s = s.replace("%", "").replace("+", "").replace("\u00a0", " ").strip()
    return s


def _is_skip_cell(cell_text: str) -> bool:
    """Check if a cell represents intentionally missing data."""
    return cell_text.lower().strip() in _SKIP_CELLS


def _parse_return_from_row(cells, diagnostics=None, return_col_idx=None):
    """
    Extract a monthly return float from a table row's cells.
    If return_col_idx is provided, use that specific column first.
    Returns (float, None) on success or (None, str) with reason on failure.
    """
    # If we know the exact column, use it directly
    if return_col_idx is not None and return_col_idx < len(cells):
        clean = _normalize_cell(cells[return_col_idx])
        if clean and not _is_skip_cell(clean):
            try:
                val = float(clean)
                if -20 <= val <= 20:
                    return val / 100.0, None
            except ValueError:
                if diagnostics is not None:
                    diagnostics.append(f"unparseable target col cell: '{clean}'")

    # Fallback: scan all cells after column 0
    for cell in cells[1:]:
        clean = _normalize_cell(cell)
        if not clean or _is_skip_cell(clean):
            continue
        try:
            val = float(clean)
            if -20 <= val <= 20:
                return val / 100.0, None
        except ValueError:
            if diagnostics is not None:
                diagnostics.append(f"unparseable cell: '{clean}'")
            continue
    return None, f"no valid return in cells: {[_normalize_cell(c) for c in cells]}"


# ── Column-header-aware parsing ──────────────────────────────────────────────

_NET_RETURN_HEADERS = {"net return", "net ret", "net ror", "net perf", "net performance",
                       "return (net)", "net of fees", "monthly net", "net return (%)",
                       "return (%)", "monthly return"}
_GROSS_RETURN_HEADERS = {"gross return", "gross ret", "gross ror", "gross perf"}
_SKIP_COL_HEADERS = {"cumulative", "cum nav", "cum. nav", "notes", "benchmark",
                     "s&p", "spy", "index", "comment", "attribution"}


def _find_return_column(header_cells):
    """
    Given a header row's cells, find the best column index for net returns.
    Priority: net return > generic return > gross return.
    Skips columns classified as cumulative/benchmark/notes.
    Returns (col_index, col_label) or (None, None).
    """
    net_col = None
    gross_col = None

    for idx, cell in enumerate(header_cells):
        if idx == 0:
            continue
        cell_l = str(cell or "").lower().strip()
        if any(kw in cell_l for kw in _SKIP_COL_HEADERS):
            continue
        if any(kw in cell_l for kw in _NET_RETURN_HEADERS):
            net_col = (idx, cell_l)
        elif any(kw in cell_l for kw in _GROSS_RETURN_HEADERS):
            gross_col = (idx, cell_l)

    if net_col:
        return net_col
    if gross_col:
        return gross_col
    return None, None


# Keywords that indicate a row is a risk/stats label, NOT a performance header
_RISK_TABLE_KEYWORDS = {"annualized", "volatility", "sharpe", "sortino", "drawdown",
                        "correlation", "beta", "alpha", "std dev", "variance",
                        "tracking error", "information ratio"}


def _is_header_row(row_text: str) -> bool:
    """Check if a row is a performance table header (not a risk metrics row)."""
    row_lower = row_text.lower()
    # Reject rows that look like risk/stats table labels
    if any(kw in row_lower for kw in _RISK_TABLE_KEYWORDS):
        return False
    return any(kw in row_lower for kw in _RETURN_HEADER_KEYWORDS)


def _detect_horizontal_table(table):
    """
    Detect horizontal/calendar format where:
      - First column has month names
      - Other columns are years (2023, 2024, 2025, ...)
    Returns the year-column index for the most recent year, or None.
    """
    if not table or len(table) < 2:
        return None
    header = [str(c or "").strip() for c in table[0]]
    year_cols = {}
    for idx, cell in enumerate(header):
        # Match "2024", "2025 YTD", etc.
        m = re.match(r"^(20\d{2})", cell)
        if m:
            year_cols[int(m.group(1))] = idx
    if year_cols:
        return year_cols[max(year_cols)]  # most recent year column
    return None


def _is_numeric_table(table, threshold=0.4):
    """
    Heuristic: detect if a table is mostly numeric (potential return table)
    even without explicit header keywords.
    Returns True if >threshold of data cells are numeric.
    """
    if not table or len(table) < 3:
        return False
    numeric_count = 0
    total_count = 0
    for row in table[1:]:  # skip header
        for cell in row[1:]:  # skip first column (labels)
            clean = _normalize_cell(cell)
            if not clean or _is_skip_cell(clean):
                continue
            total_count += 1
            try:
                float(clean)
                numeric_count += 1
            except ValueError:
                pass
    if total_count == 0:
        return False
    return (numeric_count / total_count) >= threshold


def _extract_monthly_returns_from_tables(tables: list, diagnostics: dict):
    """
    Find the performance table across all tables and extract monthly returns.
    Supports:
      - Standard vertical format (Month | Return | ...)
      - Horizontal/calendar format (Month | 2023 | 2024 | 2025)
      - ISO date cells (2025-01, 01/2025)
      - Numeric-heavy tables without standard headers

    Returns (list[float], list[str]) — returns and any extraction warnings.
    """
    all_returns = []
    warnings = []
    diagnostics["tables_inspected"] = len(tables)
    diagnostics["rows_scanned"] = 0
    diagnostics["rows_matched"] = 0
    diagnostics["rows_skipped"] = 0

    return_col_idx = None
    return_col_label = None

    for table_idx, table in enumerate(tables):
        # Check for horizontal/calendar format first
        year_col = _detect_horizontal_table(table)
        if year_col is not None:
            table_returns = []
            for row in table[1:]:  # skip header
                cells = [str(c or "").strip() for c in row]
                diagnostics["rows_scanned"] += 1
                if not cells or not _is_month_cell(cells[0]):
                    diagnostics["rows_skipped"] += 1
                    continue
                if year_col < len(cells):
                    val, err = _parse_return_from_row(["_placeholder_", cells[year_col]])
                    if val is not None:
                        table_returns.append(val)
                        diagnostics["rows_matched"] += 1
                    elif err:
                        diagnostics["rows_skipped"] += 1
                        warnings.append(f"table[{table_idx}] horizontal row skip: {err}")
            if len(table_returns) >= 6:
                all_returns = table_returns
                continue

        # Standard vertical format
        table_returns = []
        has_month_col = False
        table_return_col = None

        for row in table:
            if not row:
                continue
            cells = [str(c or "").strip() for c in row]
            row_text = " ".join(cells)
            diagnostics["rows_scanned"] += 1

            # Detect header row — marks this table as a performance table
            if _is_header_row(row_text):
                has_month_col = True
                # Use column-header-aware parsing to find the right column
                col_idx, col_label = _find_return_column(cells)
                if col_idx is not None:
                    table_return_col = col_idx
                    return_col_label = col_label
                continue

            first_cell = cells[0] if cells else ""
            is_month_row = _is_month_cell(first_cell)

            if is_month_row or has_month_col:
                cell_diag = []
                val, err = _parse_return_from_row(
                    cells, diagnostics=cell_diag, return_col_idx=table_return_col
                )
                if val is not None:
                    table_returns.append(val)
                    diagnostics["rows_matched"] += 1
                elif err and is_month_row:
                    diagnostics["rows_skipped"] += 1
                    warnings.append(f"table[{table_idx}] row parse fail: {err}")
                    if cell_diag:
                        warnings.extend(cell_diag)

        # Accept this table if it has 6+ months AND more matches than current best
        if len(table_returns) >= 6 and len(table_returns) > len(all_returns):
            all_returns = table_returns   # main performance table found
            return_col_idx = table_return_col
        elif 1 <= len(table_returns) <= 5 and all_returns:
            all_returns.extend(table_returns)  # continuation rows (split across pages)

    diagnostics["return_column"] = return_col_label or "auto (first numeric)"
    return all_returns, warnings


def _extract_monthly_returns_from_text(text: str, diagnostics: dict):
    """
    Fallback: scan raw text for month-return pairs when table extraction fails.
    Returns (list[float], list[str]) — returns and warnings.
    """
    returns = []
    warnings = []
    lines = text.splitlines()
    lines_scanned = 0

    for line in lines:
        line_l = line.lower()
        has_month = any(kw in line_l for kw in _MONTH_KEYWORDS)
        has_date = _DATE_PATTERN.search(line) is not None

        if has_month or has_date:
            lines_scanned += 1
            m = _RETURN_PATTERN.search(line)
            if m:
                try:
                    val = float(m.group(1))
                    if -20 <= val <= 20:
                        returns.append(val / 100.0)
                except ValueError:
                    warnings.append(f"text parse fail: {line.strip()[:60]}")

    diagnostics["text_lines_scanned"] = lines_scanned
    return returns, warnings


def _parse_period_header(header_text: str) -> list:
    """Parse a period header line into a list of period keys."""
    period_map = []
    text_l = header_text.lower()
    # Scan the entire line with regex to find period labels in order
    patterns = [
        (r"1[\s-]?m(?:th|onths?)\b", "1_month"),
        (r"3[\s-]?m(?:th|onths?)\b", "3_month"),
        (r"6[\s-]?m(?:th|onths?)\b", "6_month"),
        (r"\bytd\b",                  "ytd"),
        (r"1[\s-]?y(?:r|ear)\b",     "1_year"),
        (r"3[\s-]?y(?:r|ear)\b",     "3_year"),
        (r"5[\s-]?y(?:r|ear)\b",     "5_year"),
        (r"7[\s-]?y(?:r|ear)\b",     "7_year"),
        (r"10[\s-]?y(?:r|ear)\b",    "10_year"),
        (r"inception",               "since_inception"),
    ]
    # Find all matches with their positions, then sort by position
    found = []
    for pat, key in patterns:
        m = re.search(pat, text_l)
        if m:
            found.append((m.start(), key))
    found.sort()
    period_map = [key for _, key in found]
    return period_map


def _extract_summary_performance(text: str) -> dict:
    """
    Extract summary performance data from factsheet-format PDFs.
    Scans for period header lines (e.g., "1 Month  3 Months  1 Year")
    then maps percentage values from nearby return lines.

    Returns dict with available period returns, e.g.:
    {"1_month": 0.024, "1_year": 0.28, "ytd": 0.134}
    """
    summary = {}
    lines = text.splitlines()

    # First pass: find period header lines
    header_indices = []
    for i, line in enumerate(lines):
        line_l = line.lower()
        periods = _parse_period_header(line)
        if len(periods) >= 2:
            header_indices.append((i, periods))

    if not header_indices:
        return summary

    # Second pass: for each header, find the best return line within ±4 lines
    return_keywords = {"net return", "net ret", "nav", "net performance"}

    for header_idx, periods in header_indices:
        # Search lines near the header for return data
        search_start = header_idx + 1
        search_end = min(header_idx + 5, len(lines))

        for j in range(search_start, search_end):
            line = lines[j]
            line_l = line.lower()
            pcts = re.findall(r"([+-]?\d{1,3}\.\d{1,2})\s*%", line)
            if not pcts:
                continue

            # Prefer "net return" / "NAV" lines; accept any line with enough %s
            is_net = any(kw in line_l for kw in return_keywords)
            has_enough = len(pcts) >= len(periods) * 0.5

            if is_net or has_enough:
                for k, pct in enumerate(pcts):
                    if k < len(periods):
                        summary[periods[k]] = float(pct) / 100.0
                if is_net:
                    break  # prefer net over gross, stop searching

    return summary


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
                "pages_scanned":    int,
                "tables_found":     int,
                "tables_inspected": int,
                "rows_scanned":     int,
                "rows_matched":     int,
                "rows_skipped":     int,
                "method":           "table" | "text" | "summary" | "failed",
                "returns_count":    int,
                "confidence":       float,   # 0.0–1.0 trust signal
                "return_column":    str,     # which column header was used
                "warnings":         list[str],
                "summary_perf":     dict | None,
            }
        }

    Raises:
        ValueError if fewer than 3 months of returns found AND no summary data.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"PDF not found: {path}")

    text, tables = _extract_text_and_tables(path)

    # Count pages
    with pdfplumber.open(path) as pdf:
        pages_scanned = len(pdf.pages)

    fund_name = _find_fund_name(text)
    aum_mm    = _find_aum(text)
    ticker    = _derive_ticker(fund_name, path)

    diagnostics = {}

    # Try table extraction first, fall back to text scan
    returns, warnings = _extract_monthly_returns_from_tables(tables, diagnostics)
    method = "table"
    if len(returns) < 3:
        returns, text_warnings = _extract_monthly_returns_from_text(text, diagnostics)
        warnings.extend(text_warnings)
        method = "text"

    # Always try summary extraction (useful supplementary data for factsheets)
    summary_perf = _extract_summary_performance(text) or None

    # If monthly returns insufficient, use summary as primary method
    if len(returns) < 3:
        if summary_perf:
            method = "summary"
            # Use 1-month return as a single data point if available
            if "1_month" in summary_perf:
                returns = [summary_perf["1_month"]]
        else:
            method = "failed"

    # Take last 12 months if more than 12 found
    returns = returns[-12:]

    if len(returns) < 3 and not summary_perf:
        raise ValueError(
            f"Insufficient return data extracted from {path}: "
            f"found {len(returns)} months (need >= 3). "
            f"Tables inspected: {diagnostics.get('tables_inspected', 0)}, "
            f"Rows scanned: {diagnostics.get('rows_scanned', 0)}, "
            f"Rows matched: {diagnostics.get('rows_matched', 0)}. "
            f"Warnings: {warnings}"
        )

    # ── Confidence score ─────────────────────────────────────────────────────
    method_scores = {"table": 1.0, "text": 0.5, "summary": 0.3, "failed": 0.0}
    method_score = method_scores.get(method, 0.0)
    completeness_score = min(len(returns) / 12.0, 1.0)
    warning_penalty = min(len(warnings) * 0.1, 1.0)
    confidence = round(
        method_score * 0.4 + completeness_score * 0.5 + (1.0 - warning_penalty) * 0.1,
        3
    )

    return {
        "fund_id":       ticker,
        "ticker":        ticker,
        "name":          fund_name,
        "aum_mm":        aum_mm,
        "raw_returns":   returns,
        "source_format": "pdf",
        "source_path":   path,
        "extraction": {
            "pages_scanned":    pages_scanned,
            "tables_found":     len(tables),
            "tables_inspected": diagnostics.get("tables_inspected", 0),
            "rows_scanned":     diagnostics.get("rows_scanned", 0),
            "rows_matched":     diagnostics.get("rows_matched", 0),
            "rows_skipped":     diagnostics.get("rows_skipped", 0),
            "method":           method,
            "returns_count":    len(returns),
            "confidence":       confidence,
            "return_column":    diagnostics.get("return_column", "unknown"),
            "warnings":         warnings,
            "summary_perf":     summary_perf,
        },
    }


# ── CLI test ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json, sys
    path = sys.argv[1] if len(sys.argv) > 1 else "static/sample_lp_report.pdf"
    result = load_fund_from_pdf(path)
    ext = result.pop("extraction")
    print(json.dumps(result, indent=2))
    print(f"\nExtraction metadata: {json.dumps(ext, indent=2)}")

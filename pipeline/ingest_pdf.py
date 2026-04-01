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
    r"\((\d{1,3}\.\d{1,4})\)\s*%?"     # e.g. (0.91)% — parenthetical negative
    r"|([+-]?\d{1,3}\.\d{1,4})\s*%?"   # e.g. +1.82% or -0.91
)


def _parse_return_match(m) -> float:
    """Extract float from a _RETURN_PATTERN match, handling parenthetical negatives."""
    if m.group(1):  # parenthetical negative: (0.91)
        return -float(m.group(1))
    return float(m.group(2))  # standard: +1.82 or -0.91

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

# ── Currency detection ────────────────────────────────────────────────────────
_CURRENCY_SYMBOLS = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY"}
_CURRENCY_CODES = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "SGD", "HKD", "CNY", "KRW", "SEK", "NOK", "DKK"}

# European decimal format: 1.234.567,89 (periods = thousands, comma = decimal)
_EUROPEAN_NUMBER_PATTERN = re.compile(r"(\d{1,3}(?:\.\d{3})+),(\d{1,2})")


def _detect_currency(text: str) -> dict:
    """
    Detect the reporting currency of an LP report.

    Scans for:
      1. Explicit statements: "denominated in EUR", "reporting currency: GBP"
      2. Currency symbols/codes near AUM labels
      3. Most frequent currency symbol in the document

    Returns:
        {
            "currency":        str,    # ISO code: "USD", "EUR", etc.
            "currency_source": str,    # "explicit" | "aum_context" | "symbol_frequency" | "default"
        }
    """
    text_l = text.lower()
    lines = text.splitlines()

    # Signal 1: Explicit currency statement (highest confidence)
    explicit_patterns = [
        re.compile(r"(?:denominated|reported|reporting\s+currency|base\s+currency|fund\s+currency)\s*(?:in|:)\s*(\w{3})", re.IGNORECASE),
        re.compile(r"(?:all\s+(?:amounts|figures|values)\s+(?:are\s+)?in)\s+(\w{3})", re.IGNORECASE),
        re.compile(r"currency\s*:\s*(\w{3})", re.IGNORECASE),
    ]
    for pat in explicit_patterns:
        m = pat.search(text)
        if m:
            code = m.group(1).upper()
            if code in _CURRENCY_CODES:
                return {"currency": code, "currency_source": "explicit"}

    # Signal 2: Currency near AUM labels
    for line in lines:
        line_l = line.lower()
        if any(lbl in line_l for lbl in _AUM_LABELS):
            # Check for currency symbols
            for sym, code in _CURRENCY_SYMBOLS.items():
                if sym in line:
                    return {"currency": code, "currency_source": "aum_context"}
            # Check for ISO codes
            for code in _CURRENCY_CODES:
                if re.search(r'\b' + code + r'\b', line):
                    return {"currency": code, "currency_source": "aum_context"}

    # Signal 3: Most frequent currency symbol in document
    symbol_counts = {}
    for sym, code in _CURRENCY_SYMBOLS.items():
        count = text.count(sym)
        if count > 0:
            symbol_counts[code] = count

    if symbol_counts:
        dominant = max(symbol_counts, key=symbol_counts.get)
        return {"currency": dominant, "currency_source": "symbol_frequency"}

    # Default: USD (most common in LP reports)
    return {"currency": "USD", "currency_source": "default"}


def _parse_european_number(text: str) -> float:
    """Convert European-format number to float: 1.234.567,89 → 1234567.89"""
    m = _EUROPEAN_NUMBER_PATTERN.search(text)
    if m:
        integer_part = m.group(1).replace(".", "")  # remove thousands separators
        decimal_part = m.group(2)
        return float(f"{integer_part}.{decimal_part}")
    return None


_FUND_NAME_LABELS    = ("fund name", "fund:", "managed by", "fund lp", "capital fund", "macro fund")
_AUM_LABELS          = ("ending nav", "ending capital", "total nav", "fund aum",
                        "fund assets", "total assets", "net assets", "fund size")
_BEGINNING_NAV_LABELS = ("beginning nav", "beginning capital", "beginning balance",
                          "opening nav", "opening capital", "opening balance",
                          "beg. nav", "beg nav", "start nav")
_PERIOD_LABELS       = ("reporting period", "period:", "for the period")

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
    """Return (full_text, list_of_tables, page_char_counts) from all pages."""
    full_text = []
    all_tables = []
    page_char_counts = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            full_text.append(page_text)
            page_char_counts.append(len(page_text.strip()))
            tables = page.extract_tables()
            if tables:
                all_tables.extend(tables)
    return "\n".join(full_text), all_tables, page_char_counts


def _detect_ocr_needed(page_char_counts: list) -> dict:
    """
    Detect if a PDF likely needs OCR (scanned/image-based content).

    Checks text density per page. A text-based PDF page typically has 200+ chars.
    A scanned page with no text layer has 0-50 chars (maybe stray header/footer).

    Returns:
        {
            "ocr_needed":        bool,   # True if most pages lack text
            "low_text_pages":    int,    # count of pages with < 100 chars
            "total_pages":       int,
            "avg_chars_per_page": float,
            "page_char_counts":  list[int],
        }
    """
    if not page_char_counts:
        return {"ocr_needed": True, "low_text_pages": 0, "total_pages": 0,
                "avg_chars_per_page": 0.0, "page_char_counts": []}

    low_text_threshold = 100  # chars — below this, page is likely an image
    low_text_pages = sum(1 for c in page_char_counts if c < low_text_threshold)
    total = len(page_char_counts)
    avg_chars = sum(page_char_counts) / total

    # OCR needed if: majority of pages have low text, OR average is very low
    ocr_needed = (low_text_pages / total > 0.5) or (avg_chars < 80)

    return {
        "ocr_needed":         ocr_needed,
        "low_text_pages":     low_text_pages,
        "total_pages":        total,
        "avg_chars_per_page": round(avg_chars, 1),
        "page_char_counts":   page_char_counts,
    }


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
      - "$2,660,284.00" / "€2,660,284.00" / "£2,660,284.00"  (symbol prefix)
      - "$2.66M" / "€2.66M" / "$2.66 million"  (abbreviated)
      - "$2.66B" / "€2.66B" / "$2.66 billion"  (abbreviated)
      - "2,660,284 USD" / "2,660,284 EUR"  (ISO code suffix)
      - "NAV: 2,660,284"  (bare number on same or next line)
      - "2.660.284,00" (European decimal format)
      - Line-continuation where $ amount is on the next line
    """
    lines = text.splitlines()
    # Match any common currency symbol: $ € £ ¥
    _SYM = r"[\$€£¥]"
    # Match any ISO currency code
    _ISO_CODES = "|".join(_CURRENCY_CODES)

    def _try_parse_amount(search_text):
        """Try multiple AUM formats on a text string. Returns value in millions or None."""
        # Format 0: European decimal format near AUM label: 2.660.284,00
        eu_val = _parse_european_number(search_text)
        if eu_val is not None and eu_val > 100_000:
            return round(eu_val / 1_000_000, 2)
        # Format 1: [symbol]X.XXM / [symbol]X.XX million
        m = re.search(_SYM + r"([\d,.]+)\s*[Mm](?:illion)?", search_text)
        if m:
            try:
                return round(float(m.group(1).replace(",", "")), 2)
            except ValueError:
                pass
        # Format 2: [symbol]X.XXB / [symbol]X.XX billion
        m = re.search(_SYM + r"([\d,.]+)\s*[Bb](?:illion)?", search_text)
        if m:
            try:
                return round(float(m.group(1).replace(",", "")) * 1000, 2)
            except ValueError:
                pass
        # Format 3: [symbol]X,XXX,XXX.XX (standard format with currency symbol)
        m = re.search(_SYM + r"([\d,]+(?:\.\d{1,2})?)", search_text)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val > 100_000:
                    return round(val / 1_000_000, 2)
            except ValueError:
                pass
        # Format 4: X,XXX,XXX [ISO_CODE] (e.g. "2,660,284 EUR")
        m = re.search(r"([\d,]+(?:\.\d{1,2})?)\s*(?:" + _ISO_CODES + r")\b", search_text)
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
            # Try: label line ends with currency symbol and amount is at start of next line
            if any(line.rstrip().endswith(sym) for sym in _CURRENCY_SYMBOLS) and i + 1 < len(lines):
                m = re.search(r"^([\d,]+(?:\.\d{2})?)", lines[i + 1].strip())
                if m:
                    try:
                        val = float(m.group(1).replace(",", ""))
                        if val > 100_000:
                            return round(val / 1_000_000, 2)
                    except ValueError:
                        pass
    return None


def _find_beginning_nav(text: str):
    """Extract beginning NAV using same multi-format logic as _find_aum."""
    lines = text.splitlines()

    def _try_parse_amount(search_text):
        m = re.search(r"\$([\d,.]+)\s*[Mm](?:illion)?", search_text)
        if m:
            try:
                return round(float(m.group(1).replace(",", "")), 2)
            except ValueError:
                pass
        m = re.search(r"\$([\d,]+(?:\.\d{1,2})?)", search_text)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val > 100_000:
                    return round(val / 1_000_000, 2)
            except ValueError:
                pass
        m = re.search(r"([\d,]+(?:\.\d{1,2})?)\s*USD", search_text)
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
        if any(lbl in line_l for lbl in _BEGINNING_NAV_LABELS):
            result = _try_parse_amount(line)
            if result is not None:
                return result
            if i + 1 < len(lines):
                result = _try_parse_amount(lines[i + 1])
                if result is not None:
                    return result
    return None


def _extract_fees(text: str) -> dict:
    """
    Extract fee structure from LP report text.

    Scans for management fee and incentive/performance fee percentages.
    Common formats:
      - "Management Fees (1% annual)"
      - "Management Fee: 1.5%"
      - "Incentive Allocation (10%)"
      - "Performance Fee: 20%"
      - "2 and 20" / "1.5/20" shorthand

    Returns:
        {
            "mgmt_fee_pct":      float | None,  # e.g. 1.0 for 1%
            "incentive_fee_pct": float | None,  # e.g. 10.0 for 10%
            "fee_source":        str,            # which pattern matched
        }
    """
    result = {"mgmt_fee_pct": None, "incentive_fee_pct": None, "fee_source": None}
    lines = text.splitlines()

    # Pattern groups for management fee
    _mgmt_patterns = [
        # "Management Fee (1% annual)" or "Management Fees (1.5%)"
        re.compile(r"management\s+fees?\s*\(?(\d{1,2}(?:\.\d{1,2})?)\s*%", re.IGNORECASE),
        # "Management Fee: 1.5%" or "Mgmt Fee: 2%"
        re.compile(r"(?:management|mgmt)\s+fee\s*:?\s*(\d{1,2}(?:\.\d{1,2})?)\s*%", re.IGNORECASE),
        # "1.5% management fee"
        re.compile(r"(\d{1,2}(?:\.\d{1,2})?)\s*%\s*(?:management|mgmt)\s+fee", re.IGNORECASE),
    ]

    # Pattern groups for incentive / performance fee
    _incentive_patterns = [
        # "Incentive Allocation (10%)" or "Incentive Fee (20%)" or "Incentive Fee: 15%"
        re.compile(r"incentive\s+(?:allocation|fee)\s*:?\s*\(?(\d{1,2}(?:\.\d{1,2})?)\s*%", re.IGNORECASE),
        # "Performance Fee: 20%" or "Performance Fees (15%)"
        re.compile(r"performance\s+fees?\s*:?\s*\(?(\d{1,2}(?:\.\d{1,2})?)\s*%", re.IGNORECASE),
        # "20% incentive" or "20% performance fee"
        re.compile(r"(\d{1,2}(?:\.\d{1,2})?)\s*%\s*(?:incentive|performance\s+fees?)", re.IGNORECASE),
    ]

    full_text = text.lower()

    # Search for management fee
    for pat in _mgmt_patterns:
        m = pat.search(text)
        if m:
            try:
                val = float(m.group(1))
                if 0 < val <= 10:  # sanity: mgmt fee 0.1% to 10%
                    result["mgmt_fee_pct"] = val
                    result["fee_source"] = "explicit"
                    break
            except ValueError:
                pass

    # Search for incentive fee
    for pat in _incentive_patterns:
        m = pat.search(text)
        if m:
            try:
                val = float(m.group(1))
                if 0 < val <= 50:  # sanity: incentive fee up to 50%
                    result["incentive_fee_pct"] = val
                    if result["fee_source"] is None:
                        result["fee_source"] = "explicit"
                    break
            except ValueError:
                pass

    # Fallback: "X and Y" or "X/Y" fee shorthand (e.g. "2 and 20", "1.5/20")
    if result["mgmt_fee_pct"] is None and result["incentive_fee_pct"] is None:
        m = re.search(
            r"(\d{1,2}(?:\.\d{1,2})?)\s*(?:and|/|&)\s*(\d{1,2}(?:\.\d{1,2})?)",
            full_text
        )
        if m:
            try:
                a, b = float(m.group(1)), float(m.group(2))
                # Convention: smaller number is mgmt, larger is incentive
                if a < b and a <= 5 and b <= 50:
                    result["mgmt_fee_pct"] = a
                    result["incentive_fee_pct"] = b
                    result["fee_source"] = "shorthand"
            except ValueError:
                pass

    return result


def _classify_return_type(text: str, col_return_type: str = None) -> str:
    """
    Determine if extracted returns are net or gross.

    Uses multiple signals:
      1. Column header classification from _find_return_column (strongest)
      2. Text-level indicators ("net of fees", "after fees", "gross of fees")

    Returns: "net" | "gross" | "unknown"
    """
    # Signal 1: column header was explicit
    if col_return_type in ("net", "gross"):
        return col_return_type

    # Signal 2: scan text for net/gross indicators near return context
    text_l = text.lower()
    net_indicators = [
        "net of fees", "net of all fees", "after fees", "after all fees",
        "net returns", "net performance", "stated after management fees",
        "net of management", "after management fee",
    ]
    gross_indicators = [
        "gross of fees", "before fees", "gross returns", "gross performance",
        "before management fee", "before all fees",
    ]

    net_score = sum(1 for ind in net_indicators if ind in text_l)
    gross_score = sum(1 for ind in gross_indicators if ind in text_l)

    if net_score > 0 and net_score > gross_score:
        return "net"
    if gross_score > 0 and gross_score > net_score:
        return "gross"
    return "unknown"


def _reconcile_nav(beginning_nav_mm: float, ending_nav_mm: float,
                   monthly_returns: list) -> dict:
    """
    Cross-validate extracted monthly returns against stated NAV movement.

    Compounds monthly returns and checks if the result matches the
    beginning→ending NAV ratio stated in the capital account summary.

    Returns:
        {
            "beginning_nav_mm": float,
            "ending_nav_mm":    float,
            "implied_nav_mm":   float,   # beginning * compound(returns)
            "delta_pct":        float,   # abs % difference
            "reconciled":       bool,    # True if delta < 5%
        }
    """
    compound = 1.0
    for r in monthly_returns:
        compound *= (1.0 + r)
    implied_nav_mm = round(beginning_nav_mm * compound, 4)
    delta_pct = abs(implied_nav_mm - ending_nav_mm) / ending_nav_mm * 100
    return {
        "beginning_nav_mm": beginning_nav_mm,
        "ending_nav_mm":    ending_nav_mm,
        "implied_nav_mm":   round(implied_nav_mm, 4),
        "delta_pct":        round(delta_pct, 2),
        "reconciled":       delta_pct < 5.0,
    }


def _is_month_cell(cell_text: str) -> bool:
    """Check if a cell contains a month reference (name or ISO date)."""
    cell_l = cell_text.lower().strip()
    if any(kw in cell_l for kw in _MONTH_KEYWORDS):
        return True
    if _DATE_PATTERN.search(cell_text):
        return True
    return False


def _normalize_cell(cell) -> str:
    """Normalize a table cell value for return parsing.
    Handles parenthetical negatives: (0.91) → -0.91
    """
    if cell is None:
        return ""
    s = str(cell).strip()
    # Remove percentage signs, plus signs, whitespace variants
    s = s.replace("%", "").replace("+", "").replace("\u00a0", " ").strip()
    # Convert parenthetical negatives: (0.91) → -0.91
    paren_m = re.match(r"^\((\d+(?:\.\d+)?)\)$", s)
    if paren_m:
        s = "-" + paren_m.group(1)
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
_GROSS_RETURN_HEADERS = {"gross return", "gross ret", "gross ror", "gross perf",
                         "gross performance", "gross ror (%)"}
_AMBIGUOUS_RETURN_HEADERS = {"return", "performance", "monthly", "ror", "return(%)",
                              "period return", "monthly returns", "monthly ror"}
_SKIP_COL_HEADERS = {"cumulative", "cum nav", "cum. nav", "notes", "benchmark",
                     "s&p", "spy", "index", "comment", "attribution"}


def _find_return_column(header_cells):
    """
    Given a header row's cells, find the best column index for net returns.
    Priority: net return > gross return (with warning) > ambiguous (flagged).
    Skips columns classified as cumulative/benchmark/notes.
    Returns (col_index, col_label, return_type) or (None, None, None).
    return_type: "net" | "gross" | "unknown"
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
        return net_col[0], net_col[1], "net"
    if gross_col:
        return gross_col[0], gross_col[1], "gross"
    return None, None, None


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
    col_return_type = None  # "net" | "gross" | None (from column header)

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
                col_idx, col_label, ret_type = _find_return_column(cells)
                if col_idx is not None:
                    table_return_col = col_idx
                    return_col_label = col_label
                    col_return_type = ret_type
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
    diagnostics["col_return_type"] = col_return_type
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
                    val = _parse_return_match(m)
                    if -20 <= val <= 20:
                        returns.append(val / 100.0)
                except ValueError:
                    warnings.append(f"text parse fail: {line.strip()[:60]}")

    diagnostics["text_lines_scanned"] = lines_scanned
    return returns, warnings


_MONTH_ABBREVS = ["jan", "feb", "mar", "apr", "may", "jun",
                  "jul", "aug", "sep", "oct", "nov", "dec"]


def _extract_calendar_text_format(text: str, diagnostics: dict):
    """
    Extract monthly returns from 'year-as-row' calendar format found in text.

    Handles PDFs where pdfplumber fails to parse the table but the raw text
    contains rows like:
        Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec  YTD
        2024  1.85%  3.74%  2.77% ...
        2025  0.03% -4.02%  0.21% ...

    Strategy:
      1. Find a 'month header line' — contains >= 8 of the 12 month abbreviations
      2. Scan subsequent lines for year rows (starting with 20XX)
      3. Parse % values in column order, strip YTD if present
      4. Return the most recent complete year's returns (12 months preferred)

    Returns (list[float], list[str]) — returns and warnings.
    """
    returns = []
    warnings = []
    lines = text.splitlines()

    # Step 1: find month header line
    header_line_idx = None
    month_col_order = []
    for i, line in enumerate(lines):
        line_l = line.lower()
        found_months = [m for m in _MONTH_ABBREVS if re.search(r'\b' + m + r'\b', line_l)]
        if len(found_months) >= 8:
            header_line_idx = i
            month_col_order = found_months
            break

    if header_line_idx is None:
        return returns, warnings

    # Detect trailing summary column (YTD / Annual / Total) in header
    header_lower = lines[header_line_idx].lower()
    has_trailing_summary = bool(
        re.search(r'\b(?:ytd|annual|total|year)\b', header_lower)
    )

    diagnostics["calendar_text_header_idx"] = header_line_idx
    diagnostics["calendar_text_months_found"] = len(month_col_order)
    diagnostics["calendar_text_has_ytd_col"] = has_trailing_summary

    # Step 2: collect year rows below the header
    year_data = {}  # {year: [float, ...]}
    # Parenthetical negatives: (0.91)% or (0.91) → negative
    _pct_pattern = re.compile(r'\((\d{1,3}\.\d{1,2})\)\s*%?|([+-]?\d{1,3}\.\d{1,2})\s*%?')

    for line in lines[header_line_idx + 1:]:
        line_s = line.strip()
        m = re.match(r'^(20\d{2})\b', line_s)
        if not m:
            # Stop if we hit a non-year line after collecting some data
            if year_data:
                break
            continue

        year = int(m.group(1))
        # Extract all %-like values from the rest of the line
        # _pct_pattern returns (paren_group, standard_group) tuples
        values_raw = _pct_pattern.findall(line_s[4:])  # skip the year token
        parsed = []
        for paren_val, std_val in values_raw:
            try:
                if paren_val:  # parenthetical negative: (0.91) → -0.91
                    f = -float(paren_val)
                else:
                    f = float(std_val)
                if -50 <= f <= 50:   # wider range for annual returns / YTD
                    parsed.append(f)
            except ValueError:
                pass
        # Strip trailing summary (YTD/Annual) — it's NOT a monthly return.
        # For full years: 13 values (12 months + YTD) → strip last.
        # For partial years: e.g. 5 values (4 months + YTD) → strip last.
        # Without YTD column: keep all values as-is.
        if has_trailing_summary and len(parsed) > 1:
            parsed = parsed[:-1]
        year_data[year] = parsed

    if not year_data:
        return returns, warnings

    # Step 3: Flatten all months chronologically → take trailing 12
    # This is strictly more correct than picking a calendar year:
    # LP committees evaluate trailing 12 months, not Jan–Dec of a specific year.
    # For a PDF with 2024 full year + 3 months of 2025, calendar-year logic would
    # return 2024 data (silently stale), while trailing 12 returns Apr 2024–Mar 2025.
    _month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    all_monthly = []  # [(year, month_idx, raw_pct_value)]
    for year in sorted(year_data.keys()):
        for i, val in enumerate(year_data[year]):
            all_monthly.append((year, i, val))

    if not all_monthly:
        return returns, warnings

    trailing = all_monthly[-12:]
    returns = [v / 100.0 for (_, _, v) in trailing]

    # Build a human-readable period label
    sy, sm, _ = trailing[0]
    ey, em, _ = trailing[-1]
    period_label = f"{_month_names[sm]} {sy} – {_month_names[em]} {ey}"

    diagnostics["calendar_text_year_used"] = ey
    diagnostics["calendar_text_trailing_12_period"] = period_label
    diagnostics["calendar_text_months_extracted"] = len(returns)
    diagnostics["calendar_text_total_months_available"] = len(all_monthly)

    if len(returns) < 12:
        warnings.append(
            f"calendar text: only {len(returns)} months available across all years "
            f"(need 12 for full trailing 12 period)"
        )

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
            # Match parenthetical negatives and standard +/- format
            pct_matches = re.findall(r"\((\d{1,3}\.\d{1,2})\)\s*%|([+-]?\d{1,3}\.\d{1,2})\s*%", line)
            if not pct_matches:
                continue
            pcts = [-float(p) if p else float(s) for p, s in pct_matches]

            # Prefer "net return" / "NAV" lines; accept any line with enough %s
            is_net = any(kw in line_l for kw in return_keywords)
            has_enough = len(pcts) >= len(periods) * 0.5

            if is_net or has_enough:
                for k, pct in enumerate(pcts):
                    if k < len(periods):
                        summary[periods[k]] = pct / 100.0
                if is_net:
                    break  # prefer net over gross, stop searching

    return summary


def _extract_risk_metrics(text: str) -> dict:
    """
    Extract risk & performance metrics from LP report text.

    Scans for common risk metrics found in capital account statements:
      - Annualized Return
      - Annualized Volatility
      - Sharpe Ratio
      - Maximum Drawdown
      - Sortino Ratio
      - Correlation to benchmark
      - Beta to benchmark

    Returns dict of found metrics (empty dict if none found).
    """
    metrics = {}
    lines = text.splitlines()

    # Each pattern: (metric_key, regex, value_transform)
    # value_transform: "pct" = divide by 100, "raw" = use as-is
    _metric_patterns = [
        ("annualized_return",    re.compile(r"annualized\s+return[^:\d]*?([+-]?\d{1,3}\.\d{1,2})\s*%", re.IGNORECASE), "pct"),
        ("annualized_volatility", re.compile(r"annualized\s+volatility[^:\d]*?(\d{1,3}\.\d{1,2})\s*%", re.IGNORECASE), "pct"),
        ("sharpe_ratio",         re.compile(r"sharpe\s+ratio\s*(?:\([^)]*\))?\s*(?:[:=])?\s*([+-]?\d{1,2}\.\d{1,2})\b", re.IGNORECASE), "raw"),
        ("max_drawdown",         re.compile(r"max(?:imum)?\s+drawdown[^:\d]*?([+-]?\d{1,3}\.\d{1,2})\s*%", re.IGNORECASE), "pct"),
        ("sortino_ratio",        re.compile(r"sortino\s+ratio[^:\d]*?([+-]?\d{1,2}\.\d{1,2})", re.IGNORECASE), "raw"),
        ("calmar_ratio",         re.compile(r"calmar\s+ratio[^:\d]*?([+-]?\d{1,2}\.\d{1,2})", re.IGNORECASE), "raw"),
    ]

    for key, pat, transform in _metric_patterns:
        m = pat.search(text)
        if m:
            try:
                val = float(m.group(1))
                if transform == "pct":
                    val = val / 100.0
                metrics[key] = round(val, 6)
            except ValueError:
                pass

    # Benchmark correlation and beta: "Correlation to S&P 500 (SPY) -0.12"
    _benchmark_patterns = [
        (re.compile(r"correlation\s+to\s+(.+?)\s+([+-]?\d\.\d{1,2})", re.IGNORECASE), "correlation"),
        (re.compile(r"beta\s+to\s+(.+?)\s+([+-]?\d\.\d{1,2})", re.IGNORECASE), "beta"),
    ]
    for pat, metric_type in _benchmark_patterns:
        m = pat.search(text)
        if m:
            benchmark_name = m.group(1).strip().rstrip("(").strip()
            try:
                val = float(m.group(2))
                metrics[f"benchmark_{metric_type}"] = round(val, 4)
                if "benchmark_name" not in metrics:
                    metrics["benchmark_name"] = benchmark_name
            except ValueError:
                pass

    return metrics if metrics else None


def _cross_validate_annualized_return(monthly_returns: list, stated_annual: float) -> dict:
    """
    Cross-validate stated annualized return against compounded monthly returns.

    This catches discrepancies between the risk metrics section and the
    monthly performance table — a sign of data extraction error or
    report inconsistency.

    Returns:
        {
            "stated_annual":   float,  # from risk metrics section
            "computed_annual":  float,  # compounded from monthly returns
            "delta_pct":       float,  # absolute difference in percentage points
            "validated":       bool,   # True if delta < 1.0 pp
        }
    """
    compound = 1.0
    for r in monthly_returns:
        compound *= (1.0 + r)
    computed_annual = compound - 1.0
    delta_pct = abs(computed_annual - stated_annual) * 100  # in percentage points
    return {
        "stated_annual":  round(stated_annual, 6),
        "computed_annual": round(computed_annual, 6),
        "delta_pct":      round(delta_pct, 2),
        "validated":      delta_pct < 1.0,
    }


def load_fund_from_pdf(path: str) -> dict:
    """
    Main entry point. Parse an LP report PDF and return a normalized fund dict.

    Returns:
        {
            "ticker":           str,
            "name":             str,
            "aum_mm":           float | None,   # ending NAV in millions
            "beginning_nav_mm": float | None,   # beginning NAV in millions
            "raw_returns":      list[float],    # monthly returns as decimals
            "source_format":    "pdf",
            "source_path":      str,
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
                "reconciliation":   dict | None,  # NAV cross-check result
            }
        }

    Raises:
        ValueError if fewer than 3 months of returns found AND no summary data.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"PDF not found: {path}")

    text, tables, page_char_counts = _extract_text_and_tables(path)
    pages_scanned = len(page_char_counts)

    # OCR detection — flag scanned/image PDFs before parsing
    ocr_info = _detect_ocr_needed(page_char_counts)

    fund_name       = _find_fund_name(text)
    aum_mm          = _find_aum(text)
    beginning_nav_mm = _find_beginning_nav(text)
    ticker          = _derive_ticker(fund_name, path)
    fees            = _extract_fees(text)
    currency_info   = _detect_currency(text)

    diagnostics = {}

    # Try table extraction first
    returns, warnings = _extract_monthly_returns_from_tables(tables, diagnostics)
    method = "table"

    # Fallback 1: year-row calendar format from raw text (e.g. Winton-style)
    if len(returns) < 3:
        returns, cal_warnings = _extract_calendar_text_format(text, diagnostics)
        warnings.extend(cal_warnings)
        if len(returns) >= 3:
            method = "calendar_text"

    # Fallback 2: generic text scan
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

    # ── Return type classification ───────────────────────────────────────────
    col_return_type = diagnostics.get("col_return_type")
    return_type = _classify_return_type(text, col_return_type)

    if return_type == "gross":
        warnings.append(
            "Returns appear to be GROSS of fees — LP comparisons require net returns. "
            "Verify with GP or adjust using fee schedule."
        )
    elif return_type == "unknown":
        warnings.append(
            "Cannot determine if returns are net or gross of fees. "
            "Column header is ambiguous — manual verification recommended."
        )

    # ── NAV reconciliation ────────────────────────────────────────────────────
    reconciliation = None
    if beginning_nav_mm and aum_mm and returns:
        reconciliation = _reconcile_nav(beginning_nav_mm, aum_mm, returns)
        if not reconciliation["reconciled"]:
            warnings.append(
                f"NAV reconciliation failed: implied {reconciliation['implied_nav_mm']}M "
                f"vs stated {aum_mm}M (delta {reconciliation['delta_pct']}%)"
            )

    # ── Risk metrics extraction ──────────────────────────────────────────────
    risk_metrics = _extract_risk_metrics(text)

    # Cross-validate annualized return if both risk metrics and monthly returns available
    annualized_return_check = None
    if risk_metrics and "annualized_return" in risk_metrics and len(returns) >= 12:
        annualized_return_check = _cross_validate_annualized_return(
            returns, risk_metrics["annualized_return"]
        )
        if not annualized_return_check["validated"]:
            warnings.append(
                f"Annualized return cross-validation failed: stated {risk_metrics['annualized_return']:.2%} "
                f"vs computed {annualized_return_check['computed_annual']:.2%} "
                f"(delta {annualized_return_check['delta_pct']} pp)"
            )

    # ── Currency warning ──────────────────────────────────────────────────────
    if currency_info["currency"] != "USD":
        warnings.append(
            f"Non-USD currency detected: {currency_info['currency']} "
            f"(source: {currency_info['currency_source']}). "
            f"AUM is reported in {currency_info['currency']} — convert to USD for cross-fund comparison."
        )

    # ── OCR quality warning ─────────────────────────────────────────────────
    if ocr_info["ocr_needed"]:
        warnings.append(
            f"PDF appears to be scanned/image-based — {ocr_info['low_text_pages']}/{ocr_info['total_pages']} "
            f"pages have minimal text (avg {ocr_info['avg_chars_per_page']} chars/page). "
            f"Extracted data may be incomplete or unreliable. Consider OCR preprocessing."
        )
    elif ocr_info["low_text_pages"] > 0 and method != "table":
        # Partial OCR: some pages are images but not all — risky for text fallback
        warnings.append(
            f"PDF has {ocr_info['low_text_pages']}/{ocr_info['total_pages']} pages with minimal text. "
            f"Some content may be image-based. Verify extracted returns against source."
        )

    # ── Confidence score ─────────────────────────────────────────────────────
    method_scores = {"table": 1.0, "calendar_text": 0.85, "text": 0.5, "summary": 0.3, "failed": 0.0}
    method_score = method_scores.get(method, 0.0)
    completeness_score = min(len(returns) / 12.0, 1.0)
    warning_penalty = min(len(warnings) * 0.1, 1.0)
    # Reconciliation bonus: verified numbers get +0.05, failed check gets -0.2
    reconciliation_adj = 0.0
    if reconciliation is not None:
        reconciliation_adj = 0.05 if reconciliation["reconciled"] else -0.2
    # Return type penalty: unknown return type is a trust risk
    return_type_adj = 0.0
    if return_type == "unknown":
        return_type_adj = -0.15
    elif return_type == "gross":
        return_type_adj = -0.10  # known gross is less bad than unknown
    # OCR penalty: scanned PDFs are fundamentally less trustworthy
    ocr_adj = 0.0
    if ocr_info["ocr_needed"]:
        ocr_adj = -0.30  # severe: most pages are images
    elif ocr_info["low_text_pages"] > 0 and method != "table":
        ocr_adj = -0.10  # partial: some pages are images + non-table method
    confidence = round(
        min(1.0, max(0.0, method_score * 0.4 + completeness_score * 0.5
            + (1.0 - warning_penalty) * 0.1 + reconciliation_adj
            + return_type_adj + ocr_adj)),
        3
    )

    return {
        "fund_id":           ticker,
        "ticker":            ticker,
        "name":              fund_name,
        "aum_mm":            aum_mm,
        "beginning_nav_mm":  beginning_nav_mm,
        "currency":          currency_info["currency"],
        "mgmt_fee_pct":      fees["mgmt_fee_pct"],
        "incentive_fee_pct": fees["incentive_fee_pct"],
        "raw_returns":       returns,
        "risk_metrics":      risk_metrics,
        "source_format":     "pdf",
        "source_path":       path,
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
            "return_type":      return_type,
            "currency":         currency_info["currency"],
            "currency_source":  currency_info["currency_source"],
            "fee_source":       fees["fee_source"],
            "calendar_year":         diagnostics.get("calendar_text_year_used"),
            "trailing_12_period":    diagnostics.get("calendar_text_trailing_12_period"),
            "ocr_needed":       ocr_info["ocr_needed"],
            "low_text_pages":   ocr_info["low_text_pages"],
            "avg_chars_per_page": ocr_info["avg_chars_per_page"],
            "warnings":              warnings,
            "summary_perf":     summary_perf,
            "reconciliation":   reconciliation,
            "annualized_return_check": annualized_return_check,
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

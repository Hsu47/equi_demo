"""
Automated ground-truth validation for PDF extraction pipeline.

Tests all 5 supported PDF formats against known correct values.
Run: pytest tests/test_pdf_extraction.py -v
"""
import os
import pytest

from pipeline.ingest_pdf import load_fund_from_pdf

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_PDF = os.path.join(BASE_DIR, "static", "sample_lp_report.pdf")
TEST_DIR = os.path.join(BASE_DIR, "static", "test_pdfs")

# ── Ground truth ─────────────────────────────────────────────────────────────
# From generate_sample_pdf.py
GT_SAMPLE = {
    "returns": [1.82, 0.54, -0.91, 2.13, 0.38, -1.44, 3.07, 1.21, -0.67, 1.95, 0.83, 1.42],
    "aum_mm": 2.66,
    "mgmt_fee_pct": 1.0,
    "incentive_fee_pct": 10.0,
    "return_type": "net",
    "method": "table",
    "confidence_min": 0.95,
}

# From generate_test_pdfs.py
GT_MONTHLY = [1.23, -0.45, 2.10, 0.67, -1.33, 0.89, 1.55, -0.22, 0.98, 1.77, -0.56, 0.44]
GT_2024 = [0.95, -0.31, 1.44, 0.82, -1.10, 0.56, 2.01, -0.73, 0.65, 1.88, -0.42, 0.33]
GT_2025_PARTIAL = [1.15, -0.28, 0.77, 1.42]
GT_TRAILING_12 = GT_2024[4:] + GT_2025_PARTIAL


def _returns_to_pct(raw_returns):
    """Convert decimal returns to percentage for comparison."""
    return [round(r * 100, 2) for r in raw_returns]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _load(filename):
    if filename == "sample_lp_report.pdf":
        return load_fund_from_pdf(SAMPLE_PDF)
    return load_fund_from_pdf(os.path.join(TEST_DIR, filename))


# ═══════════════════════════════════════════════════════════════════════════════
# Sample LP Report (primary test PDF)
# ═══════════════════════════════════════════════════════════════════════════════

class TestSampleLPReport:
    """Ground truth validation for static/sample_lp_report.pdf"""

    @pytest.fixture(scope="class")
    def result(self):
        return _load("sample_lp_report.pdf")

    def test_returns_count(self, result):
        assert len(result["raw_returns"]) == 12

    def test_returns_exact_match(self, result):
        actual = _returns_to_pct(result["raw_returns"])
        assert actual == GT_SAMPLE["returns"], f"Expected {GT_SAMPLE['returns']}, got {actual}"

    def test_aum(self, result):
        assert result["aum_mm"] == GT_SAMPLE["aum_mm"]

    def test_mgmt_fee(self, result):
        assert result["mgmt_fee_pct"] == GT_SAMPLE["mgmt_fee_pct"]

    def test_incentive_fee(self, result):
        assert result["incentive_fee_pct"] == GT_SAMPLE["incentive_fee_pct"]

    def test_return_type_net(self, result):
        assert result["extraction"]["return_type"] == "net"

    def test_method_table(self, result):
        assert result["extraction"]["method"] == "table"

    def test_confidence_high(self, result):
        assert result["extraction"]["confidence"] >= GT_SAMPLE["confidence_min"]

    def test_no_warnings(self, result):
        assert result["extraction"]["warnings"] == []

    def test_ocr_not_needed(self, result):
        assert result["extraction"]["ocr_needed"] is False

    def test_nav_reconciled(self, result):
        recon = result["extraction"]["reconciliation"]
        assert recon is not None
        assert recon["reconciled"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# Format A: AQR-style factsheet (calendar text extraction)
# ═══════════════════════════════════════════════════════════════════════════════

class TestFormatA:
    """AQR-style factsheet with calendar year grid + summary performance."""

    @pytest.fixture(scope="class")
    def result(self):
        return _load("format_a_aqr_factsheet.pdf")

    def test_returns_count(self, result):
        assert len(result["raw_returns"]) == 12

    def test_returns_exact_match(self, result):
        actual = _returns_to_pct(result["raw_returns"])
        assert actual == GT_MONTHLY

    def test_aum(self, result):
        assert result["aum_mm"] == 850.0

    def test_method_calendar_text(self, result):
        assert result["extraction"]["method"] == "calendar_text"

    def test_confidence_reasonable(self, result):
        assert result["extraction"]["confidence"] >= 0.85

    def test_fees_extracted(self, result):
        assert result["mgmt_fee_pct"] == 1.5
        assert result["incentive_fee_pct"] == 15.0


# ═══════════════════════════════════════════════════════════════════════════════
# Format B: Ambiguous column headers
# ═══════════════════════════════════════════════════════════════════════════════

class TestFormatB:
    """LP report with 'Return (%)' instead of 'Net Return (%)'."""

    @pytest.fixture(scope="class")
    def result(self):
        return _load("format_b_ambiguous_headers.pdf")

    def test_returns_count(self, result):
        assert len(result["raw_returns"]) == 12

    def test_returns_exact_match(self, result):
        actual = _returns_to_pct(result["raw_returns"])
        assert actual == GT_MONTHLY

    def test_aum(self, result):
        assert result["aum_mm"] == 450.41

    def test_method_table(self, result):
        assert result["extraction"]["method"] == "table"

    def test_return_type_net_from_text(self, result):
        # Header is ambiguous ("Return"), but body text says "after all applicable fees"
        assert result["extraction"]["return_type"] == "net"

    def test_nav_reconciled(self, result):
        """With correct ending NAV, reconciliation should pass."""
        recon = result["extraction"]["reconciliation"]
        assert recon is not None
        assert recon["reconciled"] is True
        assert recon["delta_pct"] < 1.0  # should be ~0%


# ═══════════════════════════════════════════════════════════════════════════════
# Format C: Gross + Net columns side by side
# ═══════════════════════════════════════════════════════════════════════════════

class TestFormatC:
    """Report with both gross and net return columns — must pick net."""

    @pytest.fixture(scope="class")
    def result(self):
        return _load("format_c_gross_and_net.pdf")

    def test_returns_count(self, result):
        assert len(result["raw_returns"]) == 12

    def test_returns_exact_match(self, result):
        """Must extract NET returns, not gross."""
        actual = _returns_to_pct(result["raw_returns"])
        assert actual == GT_MONTHLY

    def test_aum(self, result):
        assert result["aum_mm"] == 1200.0

    def test_return_type_net(self, result):
        assert result["extraction"]["return_type"] == "net"

    def test_confidence_high(self, result):
        assert result["extraction"]["confidence"] >= 0.95

    def test_fees_extracted(self, result):
        assert result["mgmt_fee_pct"] == 2.0
        assert result["incentive_fee_pct"] == 20.0


# ═══════════════════════════════════════════════════════════════════════════════
# Format D: Calendar grid with partial year (trailing 12 logic)
# ═══════════════════════════════════════════════════════════════════════════════

class TestFormatD:
    """Multi-year calendar grid with 2024 full + 2025 partial — must use trailing 12."""

    @pytest.fixture(scope="class")
    def result(self):
        return _load("format_d_calendar_grid.pdf")

    def test_returns_count(self, result):
        assert len(result["raw_returns"]) == 12

    def test_trailing_12_correct(self, result):
        """Must return trailing 12 months (May 2024 – Apr 2025), NOT calendar 2024."""
        actual = _returns_to_pct(result["raw_returns"])
        assert actual == GT_TRAILING_12, (
            f"Expected trailing 12 {GT_TRAILING_12}, got {actual}. "
            f"If this matches GT_2024 instead, the trailing-12 logic is broken."
        )

    def test_not_calendar_2024(self, result):
        """Explicitly verify we did NOT return calendar year 2024."""
        actual = _returns_to_pct(result["raw_returns"])
        assert actual != GT_2024, "Parser returned calendar 2024 instead of trailing 12!"

    def test_aum(self, result):
        assert result["aum_mm"] == 3200.0

    def test_method_calendar_text(self, result):
        assert result["extraction"]["method"] == "calendar_text"

    def test_trailing_period_present(self, result):
        period = result["extraction"].get("trailing_12_period")
        assert period is not None, "trailing_12_period should be set for calendar_text method"
        assert "2024" in period and "2025" in period

    def test_ytd_not_included(self, result):
        """YTD value must NOT appear in monthly returns (it's a cumulative, not a month)."""
        actual = _returns_to_pct(result["raw_returns"])
        # YTD for 2025 partial = sum of GT_2025_PARTIAL ≈ 3.06
        ytd_approx = round(sum(GT_2025_PARTIAL), 2)
        assert ytd_approx not in actual, f"YTD value {ytd_approx} found in returns — contamination!"


# ═══════════════════════════════════════════════════════════════════════════════
# Cross-format structural tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestStructural:
    """Verify output schema consistency across all formats."""

    ALL_PDFS = [
        "sample_lp_report.pdf",
        "format_a_aqr_factsheet.pdf",
        "format_b_ambiguous_headers.pdf",
        "format_c_gross_and_net.pdf",
        "format_d_calendar_grid.pdf",
    ]

    REQUIRED_TOP_KEYS = {
        "fund_id", "ticker", "name", "aum_mm", "raw_returns",
        "source_format", "source_path", "extraction",
    }

    REQUIRED_EXTRACTION_KEYS = {
        "method", "returns_count", "confidence", "return_type",
        "warnings", "ocr_needed",
    }

    @pytest.fixture(scope="class")
    def all_results(self):
        return {name: _load(name) for name in self.ALL_PDFS}

    @pytest.mark.parametrize("pdf_name", ALL_PDFS)
    def test_has_required_top_keys(self, all_results, pdf_name):
        result = all_results[pdf_name]
        missing = self.REQUIRED_TOP_KEYS - set(result.keys())
        assert not missing, f"{pdf_name} missing keys: {missing}"

    @pytest.mark.parametrize("pdf_name", ALL_PDFS)
    def test_has_required_extraction_keys(self, all_results, pdf_name):
        ext = all_results[pdf_name]["extraction"]
        missing = self.REQUIRED_EXTRACTION_KEYS - set(ext.keys())
        assert not missing, f"{pdf_name} extraction missing keys: {missing}"

    @pytest.mark.parametrize("pdf_name", ALL_PDFS)
    def test_confidence_in_range(self, all_results, pdf_name):
        conf = all_results[pdf_name]["extraction"]["confidence"]
        assert 0.0 <= conf <= 1.0, f"{pdf_name} confidence out of range: {conf}"

    @pytest.mark.parametrize("pdf_name", ALL_PDFS)
    def test_returns_are_decimals(self, all_results, pdf_name):
        """Returns should be in decimal form (e.g. 0.0182, not 1.82)."""
        for r in all_results[pdf_name]["raw_returns"]:
            assert -0.5 <= r <= 0.5, (
                f"{pdf_name}: return {r} looks like percentage, not decimal"
            )

    @pytest.mark.parametrize("pdf_name", ALL_PDFS)
    def test_source_format_pdf(self, all_results, pdf_name):
        assert all_results[pdf_name]["source_format"] == "pdf"

    def test_nonexistent_pdf_raises(self):
        with pytest.raises(FileNotFoundError):
            load_fund_from_pdf("/nonexistent/fake.pdf")

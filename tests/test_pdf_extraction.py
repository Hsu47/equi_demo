"""
Automated regression tests for PDF extraction pipeline.

Tests all supported formats against known ground truth.
Run: pytest tests/test_pdf_extraction.py -v
"""
import os
import json
import pytest
from pipeline.ingest_pdf import load_fund_from_pdf
from pipeline.generate_test_pdfs import GROUND_TRUTH, GT_MONTHLY, GT_TRAILING_12

# ── Paths ────────────────────────────────────────────────────────────────────

SAMPLE_PDF = "static/sample_lp_report.pdf"
TEST_PDF_DIR = "static/test_pdfs"

SAMPLE_GROUND_TRUTH = {
    "monthly_returns": [1.82, 0.54, -0.91, 2.13, 0.38, -1.44, 3.07, 1.21, -0.67, 1.95, 0.83, 1.42],
    "aum_mm": 2.66,
    "beginning_nav_mm": 2.45,
    "mgmt_fee_pct": 1.0,
    "incentive_fee_pct": 10.0,
    "return_type": "net",
    "method": "table",
}


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def ensure_test_pdfs():
    """Generate test PDFs if they don't exist."""
    if not os.path.exists(TEST_PDF_DIR):
        from pipeline.generate_test_pdfs import main
        main()


# ── Sample LP Report (primary regression target) ────────────────────────────

class TestSamplePdf:
    """Tests against static/sample_lp_report.pdf — the original ground truth."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.result = load_fund_from_pdf(SAMPLE_PDF)
        self.ext = self.result["extraction"]

    def test_returns_count(self):
        assert len(self.result["raw_returns"]) == 12

    def test_returns_exact_match(self):
        expected = [r / 100 for r in SAMPLE_GROUND_TRUTH["monthly_returns"]]
        for i, (actual, exp) in enumerate(zip(self.result["raw_returns"], expected)):
            assert abs(actual - exp) < 1e-6, f"Month {i+1}: {actual} != {exp}"

    def test_aum(self):
        assert self.result["aum_mm"] == SAMPLE_GROUND_TRUTH["aum_mm"]

    def test_beginning_nav(self):
        assert self.result["beginning_nav_mm"] == SAMPLE_GROUND_TRUTH["beginning_nav_mm"]

    def test_fees(self):
        assert self.result["mgmt_fee_pct"] == SAMPLE_GROUND_TRUTH["mgmt_fee_pct"]
        assert self.result["incentive_fee_pct"] == SAMPLE_GROUND_TRUTH["incentive_fee_pct"]

    def test_return_type(self):
        assert self.ext["return_type"] == "net"

    def test_method(self):
        assert self.ext["method"] == "table"

    def test_confidence(self):
        assert self.ext["confidence"] >= 0.95

    def test_no_warnings(self):
        assert self.ext["warnings"] == []

    def test_nav_reconciliation(self):
        rec = self.ext["reconciliation"]
        assert rec is not None
        assert rec["reconciled"] is True
        assert rec["delta_pct"] < 5.0

    def test_ocr_not_needed(self):
        assert self.ext["ocr_needed"] is False

    def test_fund_name(self):
        assert "Meridian" in self.result["name"]


# ── Multi-format tests ───────────────────────────────────────────────────────

def _load_test_pdf(pdf_name):
    path = os.path.join(TEST_PDF_DIR, pdf_name)
    return load_fund_from_pdf(path)


class TestFormatA:
    """Format A: AQR-style factsheet with calendar grid + summary performance."""

    @pytest.fixture(autouse=True)
    def setup(self, ensure_test_pdfs):
        self.gt = GROUND_TRUTH["format_a_aqr_factsheet.pdf"]
        self.result = _load_test_pdf("format_a_aqr_factsheet.pdf")
        self.ext = self.result["extraction"]

    def test_returns_exact_match(self):
        expected = [r / 100 for r in self.gt["monthly_returns"]]
        actual = self.result["raw_returns"]
        assert len(actual) == 12
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert abs(a - e) < 1e-6, f"Month {i+1}: {a} != {e}"

    def test_aum(self):
        assert self.result["aum_mm"] == self.gt["aum_mm"]

    def test_method(self):
        assert self.ext["method"] == "calendar_text"

    def test_confidence_reasonable(self):
        assert self.ext["confidence"] >= 0.85

    def test_fund_name_contains(self):
        assert "Apex" in self.result["name"]


class TestFormatB:
    """Format B: Ambiguous headers ('Return' not 'Net Return')."""

    @pytest.fixture(autouse=True)
    def setup(self, ensure_test_pdfs):
        self.gt = GROUND_TRUTH["format_b_ambiguous_headers.pdf"]
        self.result = _load_test_pdf("format_b_ambiguous_headers.pdf")
        self.ext = self.result["extraction"]

    def test_returns_exact_match(self):
        expected = [r / 100 for r in self.gt["monthly_returns"]]
        actual = self.result["raw_returns"]
        assert len(actual) == 12
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert abs(a - e) < 1e-6, f"Month {i+1}: {a} != {e}"

    def test_aum(self):
        assert self.result["aum_mm"] == self.gt["aum_mm"]

    def test_return_type(self):
        assert self.ext["return_type"] == self.gt["return_type"]

    def test_nav_reconciliation_passes(self):
        """After fixing test data, reconciliation should pass."""
        rec = self.ext["reconciliation"]
        assert rec is not None
        assert rec["reconciled"] is True

    def test_confidence(self):
        assert self.ext["confidence"] >= 0.95


class TestFormatC:
    """Format C: Gross + Net side by side — must pick Net."""

    @pytest.fixture(autouse=True)
    def setup(self, ensure_test_pdfs):
        self.gt = GROUND_TRUTH["format_c_gross_and_net.pdf"]
        self.result = _load_test_pdf("format_c_gross_and_net.pdf")
        self.ext = self.result["extraction"]

    def test_returns_exact_match(self):
        """Must pick NET returns, not GROSS."""
        expected = [r / 100 for r in self.gt["monthly_returns"]]
        actual = self.result["raw_returns"]
        assert len(actual) == 12
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert abs(a - e) < 1e-6, f"Month {i+1}: {a} != {e}"

    def test_aum(self):
        assert self.result["aum_mm"] == self.gt["aum_mm"]

    def test_return_type_is_net(self):
        assert self.ext["return_type"] == "net"

    def test_confidence(self):
        assert self.ext["confidence"] >= 0.95


class TestFormatD:
    """Format D: Multi-year calendar grid with partial year — trailing 12."""

    @pytest.fixture(autouse=True)
    def setup(self, ensure_test_pdfs):
        self.gt = GROUND_TRUTH["format_d_calendar_grid.pdf"]
        self.result = _load_test_pdf("format_d_calendar_grid.pdf")
        self.ext = self.result["extraction"]

    def test_returns_trailing_12(self):
        """Must return trailing 12 (May 2024 – Apr 2025), NOT calendar 2024."""
        expected = [r / 100 for r in self.gt["monthly_returns"]]
        actual = self.result["raw_returns"]
        assert len(actual) == 12
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert abs(a - e) < 1e-6, f"Month {i+1}: {a} != {e}"

    def test_aum(self):
        assert self.result["aum_mm"] == self.gt["aum_mm"]

    def test_has_trailing_period(self):
        assert self.ext.get("trailing_12_period") is not None

    def test_method(self):
        assert self.ext["method"] == "calendar_text"

    def test_confidence(self):
        assert self.ext["confidence"] >= 0.85


# ── Edge cases ───────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_nonexistent_pdf_raises(self):
        with pytest.raises(FileNotFoundError):
            load_fund_from_pdf("nonexistent.pdf")

    def test_returns_are_decimals(self):
        """Returns should be decimals (0.0182), not percentages (1.82)."""
        result = load_fund_from_pdf(SAMPLE_PDF)
        for r in result["raw_returns"]:
            assert -0.5 < r < 0.5, f"Return {r} looks like percentage, not decimal"

    def test_confidence_bounded(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        assert 0.0 <= result["extraction"]["confidence"] <= 1.0

    def test_required_fields_present(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        required = ["fund_id", "ticker", "name", "aum_mm", "raw_returns",
                     "source_format", "source_path", "extraction"]
        for field in required:
            assert field in result, f"Missing required field: {field}"

    def test_extraction_fields_present(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        ext = result["extraction"]
        required = ["method", "confidence", "returns_count", "return_type",
                     "warnings", "ocr_needed", "sanity_checks"]
        for field in required:
            assert field in ext, f"Missing extraction field: {field}"


# ── Analytics Tests ──────────────────────────────────────────────────────────

class TestAnalytics:
    """Test computed analytics on sample PDF."""

    def test_analytics_present(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        assert "analytics" in result
        a = result["analytics"]
        required = ["cumulative_return", "annualized_return", "annualized_volatility",
                     "sharpe_ratio", "max_drawdown", "best_month", "worst_month",
                     "months_analyzed"]
        for field in required:
            assert field in a, f"Missing analytics field: {field}"

    def test_cumulative_return(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        a = result["analytics"]
        # Compound 12 monthly returns: product of (1+r) - 1
        expected = 1.0
        for r in SAMPLE_GROUND_TRUTH["monthly_returns"]:
            expected *= (1.0 + r / 100.0)
        expected -= 1.0
        assert abs(a["cumulative_return"] - expected) < 1e-4

    def test_annualized_return_positive(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        a = result["analytics"]
        # Fund has positive cumulative return → annualized should be positive
        assert a["annualized_return"] > 0

    def test_volatility_reasonable(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        a = result["analytics"]
        # Macro fund: vol should be in 2-20% range
        assert 0.02 < a["annualized_volatility"] < 0.20

    def test_sharpe_positive(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        a = result["analytics"]
        assert a["sharpe_ratio"] > 0

    def test_max_drawdown_bounded(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        a = result["analytics"]
        assert 0 <= a["max_drawdown"] <= 1.0

    def test_best_worst_month(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        a = result["analytics"]
        assert a["best_month"] > 0   # fund has positive months
        assert a["worst_month"] < 0  # fund has negative months
        assert a["best_month"] >= a["worst_month"]

    def test_months_analyzed(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        assert result["analytics"]["months_analyzed"] == 12


# ── Sanity Checks Tests ─────────────────────────────────────────────────────

class TestSanityChecks:
    """Test content-level sanity validation."""

    def test_all_checks_pass_sample(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        checks = result["extraction"]["sanity_checks"]
        assert len(checks) >= 3
        for c in checks:
            assert c["passed"], f"Sanity check '{c['check']}' failed: {c['detail']}"

    def test_sanity_check_structure(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        for c in result["extraction"]["sanity_checks"]:
            assert "check" in c
            assert "passed" in c
            assert "detail" in c

    def test_unit_function_magnitude(self):
        """Test _sanity_check_returns catches extreme magnitude."""
        from pipeline.ingest_pdf import _sanity_check_returns, _compute_analytics
        # Returns in percentage form (not divided by 100) — parser bug simulation
        bad_returns = [1.82, 0.54, -0.91, 2.13, 0.38, -1.44, 3.07, 1.21, -0.67, 1.95, 0.83, 1.42]
        analytics = _compute_analytics(bad_returns)
        checks = _sanity_check_returns(bad_returns, analytics)
        magnitude_check = [c for c in checks if c["check"] == "magnitude"][0]
        assert not magnitude_check["passed"], "Should flag returns > 30%"

    def test_unit_function_all_zeros(self):
        """Test _sanity_check_returns catches all-zero returns."""
        from pipeline.ingest_pdf import _sanity_check_returns, _compute_analytics
        zeros = [0.0] * 12
        analytics = _compute_analytics(zeros)
        checks = _sanity_check_returns(zeros, analytics)
        zero_check = [c for c in checks if c["check"] == "non_zero"][0]
        assert not zero_check["passed"]

    def test_all_formats_pass_sanity(self):
        """All test PDFs should pass all sanity checks."""
        pdfs = [SAMPLE_PDF] + [
            os.path.join(TEST_PDF_DIR, f)
            for f in os.listdir(TEST_PDF_DIR) if f.endswith(".pdf")
        ]
        for pdf in pdfs:
            result = load_fund_from_pdf(pdf)
            for c in result["extraction"]["sanity_checks"]:
                assert c["passed"], f"{pdf}: sanity '{c['check']}' failed: {c['detail']}"

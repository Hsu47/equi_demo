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

    def test_labeled_returns_present(self):
        lr = self.result["labeled_returns"]
        assert len(lr) == 12
        for entry in lr:
            assert "month" in entry and "return" in entry
            assert entry["month"] is not None

    def test_labeled_returns_months_correct(self):
        months = [lr["month"] for lr in self.result["labeled_returns"]]
        assert months[0] == "Jan 2025"
        assert months[11] == "Dec 2025"

    def test_reporting_period(self):
        assert self.result["reporting_period"] == "Jan 2025 – Dec 2025"


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

    def test_labeled_returns_trailing_12_labels(self):
        """Trailing-12 labels should span May 2024 – Apr 2025."""
        months = [lr["month"] for lr in self.result["labeled_returns"]]
        assert months[0] == "May 2024"
        assert months[-1] == "Apr 2025"
        assert len(months) == 12
        assert all(m is not None for m in months)

    def test_reporting_period_cross_year(self):
        assert self.result["reporting_period"] == "May 2024 – Apr 2025"


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
                     "labeled_returns", "reporting_period",
                     "source_format", "source_path", "extraction"]
        for field in required:
            assert field in result, f"Missing required field: {field}"

    def test_extraction_fields_present(self):
        result = load_fund_from_pdf(SAMPLE_PDF)
        ext = result["extraction"]
        required = ["method", "confidence", "returns_count", "return_type",
                     "warnings", "ocr_needed"]
        for field in required:
            assert field in ext, f"Missing extraction field: {field}"

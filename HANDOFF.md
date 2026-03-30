# HANDOFF — Three-Role Iteration (PDF Extraction v2)

## ARCHITECT → DEV: Recommendations

**Date:** 2026-03-30
**File reviewed:** `pipeline/ingest_pdf.py`
**PDFs tested:** `sample_lp_report.pdf` ✅ (6 returns), `aqr_managed_futures.pdf` ⚠️ (3 returns, text fallback), `dbmf_factsheet.pdf` ❌ (fails)

### Root Cause Analysis

The module works on the synthetic sample report but **fails on real-world PDFs** (AQR, DBMF). Three issues:

1. **Table-to-text fallback triggers too early** — AQR has 10 tables but none pass header detection
2. **Return cell parsing too strict** — no handling for dash/em-dash/N/A cells
3. **No diagnostic visibility** — empty warnings even on partial/total failure

### Recommended Implementation
1. Add numeric-table heuristic for headerless tables
2. Improve cell normalization (%, —, N/A)
3. Add factsheet summary extraction for non-LP-report PDFs
4. Add structured extraction diagnostics
5. Better error messages on failure

---

## DEV → QA: Ready for Review

**Date:** 2026-03-30
**Changes committed to:** `feat/pdf-extraction-v2`

### What Changed

1. **Summary/factsheet extraction** (`_extract_summary_performance`): New extraction mode for PDFs that only have period summary tables (1-Mth, 3-Mth, 1-Yr, etc.) instead of monthly returns. Handles both AQR and DBMF/TAIL factsheet formats.

2. **Robust period header parsing** (`_parse_period_header`): Regex-based scanner that finds period labels regardless of spacing/formatting. Handles "1 Month", "1-Mth", "3 Months", "1 Year", "5-Yr", "Inception" etc.

3. **Cell normalization** (`_normalize_cell`, `_is_skip_cell`): Handles `%`, `—`, `–`, `N/A`, `None` cells gracefully instead of silently failing.

4. **Improved fund name detection**: Skips lines with "inception", "expense", etc. that were being mistaken for fund names. DBMF now correctly identified as "TAIL Cambria Tail Risk ETF".

5. **Structured extraction diagnostics**: `extraction` metadata now includes `tables_inspected`, `rows_scanned`, `rows_matched`, `rows_skipped` for debugging.

6. **Richer error messages**: ValueError now reports tables inspected, rows scanned/matched counts.

### Test Results

| PDF | Method | Returns | Summary Periods | Fund Name | Status |
|-----|--------|---------|-----------------|-----------|--------|
| sample_lp_report.pdf | table | 6 | — | Meridian Global Macro Fund LP | ✅ |
| aqr_managed_futures.pdf | text | 3 | 7 (1m→10yr) | AQR Wholesale Managed Futures Fund – Class 1P | ✅ |
| dbmf_factsheet.pdf | summary | 1 | 5 (1m→5yr) | TAIL Cambria Tail Risk ETF | ✅ |

### Ready for QA
- All 3 PDFs parse without errors
- `/api/pdf_demo` endpoint should work with sample_lp_report.pdf (primary path unchanged)
- Verify: accuracy of extracted values, usability of summary data for investor decisions

# RESEARCH.md — PDF Extraction v2

## 1. Existing Extractor Analysis (`pipeline/ingest_pdf.py`)

### Current Capabilities
- Uses **pdfplumber** for text + table extraction
- Supports vertical table (Month | Return), horizontal/calendar table, ISO dates
- Summary/factsheet fallback for non-standard PDFs
- Raw text scanning as last resort
- Extracts: fund name, AUM (ending NAV), monthly returns
- Diagnostics dict tracks rows scanned/matched/skipped

### Current Gaps
1. **No extraction confidence score** — downstream has no way to know if data is trustworthy
2. **Column header confusion** — `_parse_return_from_row` takes the first numeric cell after column 0; if "Gross Return" comes before "Net Return", it picks gross
3. **No column-header-aware parsing** — doesn't map header labels to column indices
4. **AUM parsing fragile** — only handles `$X,XXX.XX` format, misses `$2.66M` or `2,660,284 USD`
5. **No multi-table disambiguation** — if PDF has both performance table AND risk metrics table, both get scanned and may collide
6. **No validation against expected month count** — silently returns partial data (e.g. 9 months) without warning
7. **No IRR/TVPI/DPI extraction** — only monthly returns, missing key LP decision metrics
8. **No fee extraction** — management fee %, incentive allocation % not captured

---

## 2. Competitive Landscape

### Canoe Intelligence
- Market leader in alts document processing; 1M+ docs/month, 200M+ data points, 44K+ funds
- Uses NLP, text anchoring, spatial/coordinate recognition, ML pattern generation, advanced table detection
- Handles: capital calls, distribution notices, K-1s, fact sheets, quarterly financials, manager letters
- **Key insight**: No standardization exists across GP reports — Canoe's moat is breadth of format coverage + human-in-the-loop QA
- **Limitation**: Proprietary, enterprise pricing ($100K+/yr), not available as API for startups

### Open-Source Tools Comparison
| Tool | Strengths | Weaknesses |
|------|-----------|------------|
| **pdfplumber** (current) | Clean machine-generated PDFs, good text layer | No OCR, limited table detection heuristics |
| **Camelot** | Better lattice table parsing than Tabula | Unmaintained (5 yrs), no OCR |
| **Tabula-py** | Good stream table detection | Worse lattice parsing, Java dependency |
| **AWS Textract** | OCR + table extraction, cloud-scale | Cost per page, latency, overkill for structured PDFs |
| **LLMWhisperer** | Best accuracy in recent benchmarks | API dependency, cost |
| **TableFormer** | 93.6% accuracy (vs Tabula 67.9%, Camelot 73.0%) | Requires model deployment |

### Production Path Assessment
For Equi's demo stage, **pdfplumber is sufficient** — our sample PDFs are machine-generated with clean text layers. Production would need OCR (Textract or LLMWhisperer) for scanned documents.

---

## 3. LP Report Format Characteristics

### Capital Account Statement (standard fields per ILPA)
- Beginning NAV / capital balance
- Capital contributions (calls)
- Capital withdrawals / distributions
- Net income/loss allocation
- Management fees
- Incentive allocation / carried interest
- Ending NAV / capital balance
- Ownership percentage

### Performance Metrics (required for allocation decisions)
| Metric | Why It Matters |
|--------|----------------|
| **Monthly/Quarterly Net Returns** | Time-series for risk analysis, correlation, Sharpe |
| **NAV** | Current position size |
| **IRR (Net)** | Annualized return accounting for timing of cash flows |
| **TVPI** | Total value vs paid-in (realized + unrealized) |
| **DPI** | Cash-on-cash return (realized only) |
| **Max Drawdown** | Downside risk indicator |
| **Sharpe / Sortino** | Risk-adjusted return |
| **Correlation to benchmarks** | Diversification value |

### What LPs Actually Need for Allocation Decisions
1. **Monthly net returns** (12+ months) — for risk modeling
2. **AUM / NAV** — position sizing
3. **Fee structure** — to compute gross-to-net
4. **Strategy classification** — for bucket assignment
5. **Liquidity terms** — redemption constraints
6. **IRR / TVPI / DPI** — for PE/VC (less relevant for liquid alts)

---

## 4. Equi Extraction Spec

### Must-Have Fields (v2)
| Field | Current Status | Priority |
|-------|---------------|----------|
| Monthly net returns (12 mo) | Implemented but column-confused | P0 — fix |
| AUM / Ending NAV | Implemented but fragile | P1 — harden |
| Fund name | Implemented | OK |
| Extraction confidence score | Missing | P0 — add |
| Column header mapping | Missing | P0 — add |

### Should-Have Fields (v3+)
| Field | Status |
|-------|--------|
| Fee structure (mgmt %, incentive %) | Not extracted |
| IRR / TVPI / DPI | Not extracted |
| Strategy classification | Not extracted |
| Liquidity terms | Not extracted |
| Benchmark returns (separate from fund) | Not distinguished |

### Accuracy Requirements
- Monthly returns: **100% match** to source PDF values (zero tolerance for production)
- AUM: within **$1** of stated ending NAV
- Confidence score: must flag when extraction is uncertain (< 12 months, text fallback, ambiguous columns)

### Format Coverage (current)
- Machine-generated vertical table PDFs: **working**
- Horizontal/calendar table PDFs: **working**
- Scanned/image PDFs: **not supported** (no OCR)
- Multi-page split tables: **partial** (continuation logic exists)
- PDFs with multiple numeric tables (perf + risk): **risk of collision**

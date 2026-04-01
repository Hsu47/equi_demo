# HANDOFF.md — PDF Extraction v2

---

## ARCHITECT → DEV: Top 3 Improvements (Priority Order)

### 1. Column-Header-Aware Parsing (P0 — Correctness)
**Problem**: `_parse_return_from_row()` blindly takes the first numeric value after column 0. If the performance table has columns `[Month, Gross Return, Net Return, Cumulative NAV, Notes]`, it picks **Gross Return** instead of **Net Return**. In our sample PDF, the columns are `[Month, Net Return (%), Cumulative NAV, Notes]` — the parser happens to work because Net Return is first, but it would break on any PDF with a Gross column preceding Net.

**Fix**:
- After detecting a header row, parse column headers and build a `col_index` map
- Prioritize "Net Return" column; fall back to "Gross Return" only if Net is absent
- Skip columns identified as "Cumulative NAV", "Notes", "Benchmark", etc.
- Store which column was used in diagnostics (`extraction.return_column`)

**Acceptance**: Parser must correctly extract Net Return even when Gross Return column exists before it.

---

### 2. Extraction Confidence Score (P0 — Trust Signal)
**Problem**: Downstream consumers (scoring pipeline, UI) have no way to know if extracted data is reliable. A PDF that yields 8/12 months via text fallback looks the same as one with 12/12 via clean table parse.

**Fix**: Add `extraction.confidence` (0.0–1.0) computed from:
- `method_score`: table=1.0, text=0.5, summary=0.3, failed=0.0
- `completeness_score`: `returns_count / 12`
- `warning_penalty`: -0.1 per warning (floor 0)
- Final: `confidence = method_score * 0.4 + completeness_score * 0.5 + (1 - warning_penalty) * 0.1`

**Acceptance**: 12/12 months via table extraction → confidence >= 0.95. Text fallback with 8 months → confidence ~0.55.

---

### 3. Robust AUM Parsing (P1 — Edge Cases)
**Problem**: `_find_aum()` only matches `$X,XXX.XX` format. Real LP reports may use:
- `$2.66M` or `$2.66 million`
- `2,660,284 USD` (no $ prefix)
- `NAV: 2,660,284` (no currency symbol at all)
- Amount on a different line from the label

**Fix**:
- Add regex for `$X.XXM` / `$X.XX million` / `$X.XXB` format
- Support `USD` suffix as alternative to `$` prefix
- Support bare numbers on the line following an AUM label
- Normalize all to `aum_mm` (millions)

**Acceptance**: All four formats above must parse correctly to the same value.

---

## Implementation Notes for Dev
- All changes in `pipeline/ingest_pdf.py` only
- Do NOT change the return dict structure (add fields, don't remove)
- The `extraction` sub-dict can grow freely
- Test against `static/sample_lp_report.pdf` — must still get 12/12 months, correct AUM
- Ground truth from `generate_sample_pdf.py`:
  ```
  Returns: [+1.82, +0.54, -0.91, +2.13, +0.38, -1.44, +3.07, +1.21, -0.67, +1.95, +0.83, +1.42]
  AUM: $2,660,284.00 → 2.66 (in millions)
  ```

---

## DEV → QA: Ready for Review

**Commit**: `a6eb88b` — `feat: column-aware parsing, confidence score, robust AUM extraction`

### What was implemented:
1. **Column-header-aware parsing** — `_find_return_column()` maps header cells, prioritizes "Net Return" over "Gross Return", skips cumulative/benchmark columns. Stored in `extraction.return_column`.
2. **Risk-table disambiguation** — `_RISK_TABLE_KEYWORDS` prevents rows with "annualized", "volatility", "sharpe" etc. from triggering performance header detection. Table selection now prefers the table with the most matches.
3. **Confidence score** — `extraction.confidence` (0.0–1.0) computed from method, completeness, and warnings.
4. **Robust AUM parsing** — `_try_parse_amount()` handles $X.XXM, $X.XXB, X USD, bare numbers.

### Test results:
```
Returns: 12/12 months — all match ground truth exactly
AUM: 2.66 (correct)
Confidence: 1.0
Return column: "net return (%)"
Method: table
Warnings: none
```

---

## QA → ARCHITECT: Approved with Notes

### QA Results (LP Investor Perspective)

**PASS** — All acceptance criteria met:

| Check | Result |
|-------|--------|
| 12 monthly returns match ground truth | 12/12 exact match |
| AUM correct ($2,660,284 → 2.66M) | Correct |
| Confidence score meaningful (1.0 for perfect extraction) | Correct |
| Return column identified ("net return (%)") | Correct |
| Non-existent PDF raises clear error | FileNotFoundError raised |
| Risk metrics table NOT confused with perf table | Correctly disambiguated |

### Notes for Future Iterations
1. **No OCR** — scanned PDFs will fail silently (returns empty). Production needs Textract or similar.
2. **Single-format tested** — only the sample PDF was tested. Need real GP reports (varying formats) for robustness validation.
3. **No fee extraction** — management fee (1%) and incentive (10%) are visible in the PDF but not extracted. Relevant for net-to-gross reconciliation.
4. **Confidence floor** — a PDF with 3/12 months via text fallback gets confidence ~0.37, which is low but might still be useful to flag rather than reject entirely.

---

## Iteration 1 — 2026-03-30

### ARCHITECT
第一性原理問題：confidence=1.0 的根據是什麼？
目前只是「過程指標」（抓了 12 個月、用 table 方法），不是「結果指標」。
真正有意義的驗證是：把月報酬複利算出來，看有沒有對上 capital account 的起末淨值。
如果對不上 → 我們抓錯欄了，或漏月份了。這才是可信度的真正來源。

### DEV
- 新增 `_find_beginning_nav()` — 抓起始淨值（$2.45M）
- 新增 `_reconcile_nav()` — 複利驗算，計算 delta%
- 新欄位：`beginning_nav_mm`（主 dict）、`extraction.reconciliation`
- confidence 邏輯：reconciled → +0.05，failed → -0.2，上限 1.0

### QA
- 12/12 月份全對，AUM 正確，beginning NAV 正確
- 驗算 delta = 1.98%（費用拖累，合理），reconciled=true
- confidence = 1.0（上限 clip 正確）
- 委託：這個數字現在有「自我驗算」，不只是聲稱自己對

### Next
- 費用提取（mgmt fee %, incentive %）— 讓 gross-to-net 可計算
- 或：生成第二種格式的測試 PDF（橫向日曆表），驗證格式韌性
- 問自己：下一輪最危險的靜默錯誤是什麼？

---

## Iteration 2 — 2026-03-30

### ARCHITECT
第一性原理問題：我們說「支援橫向格式」，但 `_detect_horizontal_table()` 從來沒真正被測試過。
去找了真實 LP 報告——Winton Trend Fund (UCITS) 月報——直接打我們的臉：
- pdfplumber 只能抓到 header 行，data 行完全消失
- text fallback 抓到 7 個垃圾數字（NAV/fees 的數字混入）
- confidence 正確降到 0.59（confidence 機制有效）

真正的問題：Winton 用 rows=年份, cols=月份（Type B），我們的程式只處理 rows=月份, cols=年份（Type A）。

### DEV
- 新增 `_extract_calendar_text_format()` — 直接從 raw text 解析年行格式
  - 偵測含 >=8 月份縮寫的 header 行
  - 收集 20XX 開頭的年份資料行
  - 取最近有 >=6 個月的年份
- 新方法等級：`calendar_text`（confidence weight = 0.85）
- 新增 AUM 標籤：fund assets, total assets, net assets, fund size
- extraction dict 新增 `calendar_year` 欄位

### QA（LP 投資人視角）
真實 Winton 2025 月報測試：
- 12/12 月份全對（對照 PDF 原文數字）
- AUM: $1.0bn 正確（fund assets 標籤）
- confidence = 0.94（合理—無 LP 帳戶起末淨值，不能 reconcile）
- 不再有垃圾數字混入
- 兩種格式並存，不互相干擾

### Next
- 考慮：費用結構提取（mgmt fee %, incentive %）—Winton PDF 已有完整費用表
- 考慮：trailing 12 months 跨年計算（目前只取日曆年）
  如 Jan 2026 = 最新月，LP 真正想看的是 Feb 2025–Jan 2026 的 trailing 12
- 問自己：下一輪哪種靜默錯誤還沒被發現？

---

## Iteration 3 — 2026-03-31

### ARCHITECT
第一性原理問題：confidence=1.0、no warnings、12 months — 這表示「正確」嗎？

不一定。最危險的靜默錯誤是 **Calendar Year vs Trailing 12**：

- 舊邏輯：取「最近有 >=6 個月的那個日曆年」
- 如果 PDF 有 2024 全年 + 2025 的 1-3 月 → 回傳 2024 年資料（12 months, confidence=1.0, no warnings）
- LP 委員會看到「12 months return」→ 以為是 trailing 12，實際上是 calendar 2024
- 這會直接影響基金比較和配置決策
- 錯誤的可怕之處：完全靜默，沒有任何提示

CalPERS 等機構標準：trailing 12 months ending most recent month，不是 calendar year。

### DEV
- 重寫 `_extract_calendar_text_format()` 的 Step 3
- 舊：`for year in sorted(year_data.keys(), reverse=True): if len(vals) >= 6: return vals`
- 新：flatten all_monthly = [(year, month_idx, value)], 取最後 12 個
- 新增 diagnostics: `calendar_text_trailing_12_period`（e.g. "Apr 2024 – Mar 2025"）、`calendar_text_total_months_available`
- 新 extraction 欄位：`trailing_12_period`（人類可讀的期間標籤）
- `load_fund_from_pdf()` return dict 新增 `trailing_12_period`

### QA
測試結果：

| 情境 | 舊行為 | 新行為 |
|------|-------|-------|
| 2024 full + 3 months 2025 | 2024 calendar year (12m, 靜默錯誤) | Apr 2024 – Mar 2025 (trailing 12) ✓ |
| 2025 full year | Jan–Dec 2025 ✓ | Jan 2025 – Dec 2025 ✓ (no regression) |
| sample_lp_report.pdf (table method) | 12/12 exact match ✓ | 12/12 exact match ✓ (unaffected) |

sample_lp_report.pdf 回歸測試：
- 12/12 月份全對（ground truth exact match）
- AUM = 2.66 ✓
- Confidence = 1.0 ✓
- trailing_12_period = None（table method 不受影響，正確）

LP 視角評估（CalPERS alts team）：
- trailing_12_period 欄位讓分析師一眼看出「這是哪 12 個月」
- 不再有「以為是 trailing 12 實際上是 calendar year」的風險
- 字串 "Apr 2024 – Mar 2025" 直接放進報告，可稽核

### Next
下一輪最危險的靜默錯誤是什麼？

**費用結構提取（silent fee contamination）**：
- 目前我們只抓 net return，但如果某個 PDF 的欄位標示模糊
  （e.g. "Return" 而非 "Net Return"），我們可能抓到 gross return
- gross vs net 的差距對一個 2/20 基金是 ~2-3%/年
- 這是靜默的：數字長得很合理（+8% 而非 +5.5%），沒有 warning

建議 v2.2：費用提取（mgmt fee %, incentive %）+ gross/net 標記
- 抓費用表（常見格式：「Management Fee: 2%」）
- 在 extraction dict 標記 `return_type: "net" | "gross" | "unknown"`
- 如果 return_type = "unknown" → confidence 降低 0.15

---

## Iteration 4 — 2026-03-31

### ARCHITECT
第一性原理問題：如果我們抓到的數字是 gross return 而非 net return，LP 委員會會怎樣？

答案：他們會高估基金表現 2-3%/年（以 2/20 基金為例），可能做出錯誤的配置決策。
而且這個錯誤是**完全靜默的**——數字看起來非常合理，沒有任何 warning。

最危險的情境：PDF 表格標題只寫 "Return" 或 "Monthly Performance"（不是 "Net Return"），
我們的 parser 照抓不誤，confidence=1.0，no warnings。LP 以為是 net，實際是 gross。

解法不只是「標記 net/gross」，而是三個維度：
1. **費用結構提取** — 知道費率才能估算 gross-to-net 的差距
2. **Return type 分類** — 從欄位標題 + 全文語境判斷 net/gross/unknown
3. **信心懲罰** — unknown return type 直接降 confidence，逼使用者手動確認

### DEV
新增三個函數：
- `_extract_fees(text)` — 解析管理費和激勵費，支援多種格式：
  - "Management Fees (1% annual)" → mgmt_fee_pct=1.0
  - "Incentive Allocation (10%)" → incentive_fee_pct=10.0
  - "2 and 20" / "1.5/20" 速記格式
- `_classify_return_type(text, col_return_type)` — 多信號分類：
  - 信號 1：欄位標題（_find_return_column 第三個回傳值）
  - 信號 2：全文關鍵字（"net of fees", "after fees" vs "gross of fees"）
- `_find_return_column()` 擴充為回傳 3-tuple (idx, label, return_type)

Confidence 調整：
- return_type="unknown" → -0.15
- return_type="gross" → -0.10（已知 gross 比未知好，至少有明確標記）

Output 新增欄位：
- `mgmt_fee_pct`, `incentive_fee_pct`（主 dict）
- `return_type`, `fee_source`（extraction dict）

### QA（LP 投資人視角 — CalPERS alts team）
sample_lp_report.pdf 回歸測試：

| Check | Result |
|-------|--------|
| 12 monthly returns match ground truth | 12/12 exact match |
| AUM = 2.66 | Correct |
| Confidence = 1.0 | Correct |
| mgmt_fee_pct = 1.0 | Correct (PDF: "Management Fees (1% annual)") |
| incentive_fee_pct = 10.0 | Correct (PDF: "Incentive Allocation (10%)") |
| return_type = "net" | Correct (column: "net return (%)") |
| fee_source = "explicit" | Correct |
| No regressions | Confirmed |

LP 視角評估：
- **費用可見性**：分析師可以直接看到 1/10 的費用結構，不需要翻 PPM
- **Return type 標記**：明確知道「這是 net return」— 不再靠猜
- **Ambiguous 情境的 warning**：如果下一份 PDF 欄位只寫 "Return"，
  confidence 會降到 ~0.85 並附帶 warning，分析師知道要手動確認
- **Cross-validation potential**：有了費率 + return type，未來可以做
  gross→net 反算驗證（如果同時有 gross 和 net 欄位）

### Next
下一輪最危險的靜默錯誤是什麼？

**OCR 偵測（scanned PDF 靜默失敗）**：
- 目前如果 PDF 是掃描件（圖片，沒有文字層），pdfplumber 會回傳空字串
- Parser 走到 "failed" 方法，raise ValueError — 這至少不靜默
- 但如果是「部分文字層」（表格是圖片，摘要有文字）→ summary method 會抓到部分數據
  confidence 看起來合理，但月報酬是空的或不完整
- 建議 v2.4：偵測 PDF 是否有足夠文字層，沒有的話標記 `ocr_needed: true`

或者更有 demo 價值的：
- **v2.5: Format Registry** — 記住每個 GP 的 PDF 格式指紋，
  下次遇到同 GP 的報告直接套用已知的解析策略，提高穩定性
- **v2.6: LLM fallback** — 低信心 PDF 送 Claude API 做結構化提取

---

## Iteration 5 — 2026-03-31

### ARCHITECT
第一性原理問題：什麼情況下 parser 會靜默給出錯誤數據，而且看起來很合理？

答案：**部分文字層的掃描 PDF**。最危險的不是「完全掃描」（那至少會 raise ValueError），
而是「封面/摘要有文字，表格是圖片」的混合 PDF。這種情況下：
- pdfplumber 能抽到部分文字 → 不會完全失敗
- text fallback 從摘要頁抓到隨機數字（可能是費用、NAV、benchmark 數字）
- confidence 可能在 0.4-0.6 之間 — 看起來「低但不是零」
- LP 分析師可能認為「數據品質一般但可用」→ 實際上是垃圾數字

解法：在解析前就偵測文字密度，標記 OCR 需求，主動降低信心。

### DEV
新增兩個元件：
- `_extract_text_and_tables()` 現在回傳 `page_char_counts`（每頁字元數）
- `_detect_ocr_needed(page_char_counts)` — 分析文字密度：
  - `low_text_threshold = 100 chars` — 低於此的頁面視為「可能是圖片」
  - `ocr_needed = True` 當 >50% 頁面低文字 OR 平均 <80 chars/page
  - 回傳 `ocr_needed`, `low_text_pages`, `avg_chars_per_page`
- Confidence 調整：fully scanned → -0.30，partial + non-table method → -0.10
- 新 extraction 欄位：`ocr_needed`, `low_text_pages`, `avg_chars_per_page`
- 移除了 `pdfplumber.open()` 的重複呼叫（原本開兩次 PDF，現在只開一次）

### QA（LP 投資人視角 — CalPERS alts team）
sample_lp_report.pdf 回歸測試：

| Check | Result |
|-------|--------|
| 12 monthly returns match ground truth | 12/12 exact match |
| AUM = 2.66 | Correct |
| Confidence = 1.0 | Correct |
| ocr_needed = false | Correct (avg 882.5 chars/page) |
| low_text_pages = 0 | Correct |
| mgmt_fee_pct = 1.0, incentive_fee_pct = 10.0 | Correct |
| return_type = "net" | Correct |
| No regressions | Confirmed |

Edge case 驗證（模擬資料）：

| 情境 | ocr_needed | 行為 |
|------|-----------|------|
| 完全掃描（5頁, avg 16 chars） | true | -0.30 confidence, warning 產生 |
| 混合 PDF（2/4 頁低文字） | false | 如果非 table method → -0.10 + warning |
| 健康文字 PDF（avg 983 chars） | false | 無調整 |
| 空 PDF（0 頁） | true | 正確標記 |

LP 視角評估：
- 分析師上傳掃描件 → 立即看到 `ocr_needed: true` + 明確 warning
- 不再有「部分抓取看起來可用但實際是垃圾」的風險
- confidence 下降幅度合理：-0.30 讓 scanned PDF 的 confidence 不可能超過 0.7
- `avg_chars_per_page` 提供可稽核的數字，不只是 boolean flag

### Next
下一輪最危險的靜默錯誤是什麼？

**LLM fallback（v2.6）的時機到了**：
- 我們現在有完整的 confidence 機制（method + completeness + NAV reconciliation + fee/return type + OCR）
- 當 confidence < 0.6（或 ocr_needed=true），與其給出不可靠的數據，
  不如直接把 PDF 送 Claude API 做結構化提取
- Claude 可以「看」圖片，處理掃描件，理解非標準格式
- 這是 demo 的殺手級功能：「連掃描件都能處理」

或者更穩健的方向：
- **v2.5: Format Registry** — 記住已知格式，避免每次重新偵測
  對 production 系統更有價值（同 GP 的報告格式通常不變）
- **多 PDF 批次測試** — 找 3-5 份真實 GP 報告，驗證所有路徑的韌性

---

## Iteration 6 — 2026-03-31

### ARCHITECT
第一性原理問題：我們有 4 種格式的測試 PDF（Iteration 5 加的 test generator），但從來沒跑過 parser 驗證。這是最基本的 validation gap——**你寫了測試但沒跑測試**。

跑了之後發現：Format D（calendar grid with partial year + YTD column）靜默地給出錯誤數據。

具體錯誤：
- 2025 年只有 Jan-Apr 4 個月數據，但 YTD 欄位有 3.06%
- Parser 的 `_pct_pattern` 跳過 dash（`-`）但抓到 YTD（3.06），當成第 5 個月
- 舊邏輯只在 `len(parsed) == 13` 時才砍 YTD → partial year (5 values) 不砍
- 結果：trailing-12 window 偏移 1 個月，YTD 值被當成月報酬
- **零 warning，confidence = 0.94**——LP 委員會完全不會察覺

這是教科書級的靜默數據汙染：一個累計數字偽裝成月度數字，數值大小完全合理（3.06% 作為月報酬很正常）。

### DEV
修改 `_extract_calendar_text_format()`：
- 新增 header 行的 YTD/Annual/Total 偵測：`has_trailing_summary`
- 改寫 YTD 剝離邏輯：從「只在 13 個值時砍最後一個」→「有 YTD 欄就永遠砍最後一個」
- 新增 diagnostics 欄位：`calendar_text_has_ytd_col`
- 同時跑完所有 5 份 PDF（1 sample + 4 format variants），全部 PASS

### QA（LP 投資人視角 — CalPERS alts team）

全格式測試結果：

| PDF | Returns | AUM | Confidence | Method | Notes |
|-----|---------|-----|------------|--------|-------|
| sample_lp_report.pdf | 12/12 exact ✓ | 2.66 ✓ | 1.0 | table | No regression |
| format_a (AQR factsheet) | 12/12 exact ✓ | 850.0 ✓ | 0.94 | calendar_text | YTD correctly stripped |
| format_b (ambiguous headers) | 12/12 exact ✓ | 420.0 ✓ | 0.79 | table | return_type=net via text context |
| format_c (gross + net cols) | 12/12 exact ✓ | 1200.0 ✓ | 1.0 | table | Correctly picks net column |
| format_d (calendar grid) | 12/12 exact ✓ | 3200.0 ✓ | 0.94 | calendar_text | **Fixed**: trailing May 2024–Apr 2025 |

前後比較（Format D）：

| | 舊行為 | 新行為 |
|---|--------|--------|
| Trailing period | Jun 2024 – May 2025 (wrong) | May 2024 – Apr 2025 (correct) |
| Last value | 3.06% (YTD, not a month!) | 1.42% (Apr 2025, correct) |
| First value | 0.56% (Jun 2024) | -1.10% (May 2024) |
| Warning | None (silent!) | None (correct data, no warning needed) |

LP 視角：
- 這個修復防止了一個會直接影響基金比較的靜默錯誤
- 3.06% YTD 被當成月報酬 → 年化 ~36%，會嚴重高估基金表現
- 現在有 5 種格式的自動化 ground truth 驗證，parser 的可信度大幅提高

### Next
下一輪最危險的靜默錯誤是什麼？

**Format B 的 NAV reconciliation 看起來有問題**：
- Beginning NAV = $420M, Ending NAV = $420M（相同，PDF 設計錯誤？）
- 但 12 個月回報加總 ≠ 0% → implied NAV = $450.4M vs stated $420M (delta 7.24%)
- 這觸發了 reconciliation warning，confidence 降到 0.79
- 問題是：這是「測試 PDF 的 ground truth 設計錯誤」還是「parser 的問題」？
- 需要修正 test PDF generator 的 ground truth（ending NAV 應該 = beginning * compound returns）

其他方向：
- **v2.6: LLM fallback** — 低信心 PDF 送 Claude API（demo 殺手級功能）
- **v2.5: Format Registry** — 記住 GP 格式指紋（production 價值高）
- **自動化測試套件** — 把 5 種格式的驗證包成 pytest，CI 可跑

---

## Iteration 7 — 2026-04-01

### ARCHITECT
第一性原理問題：我們有 6 輪迭代的改進，5 種 PDF 格式，但**零自動化回歸測試**。

每次迭代都是手動跑測試，容易漏掉。Iteration 6 的 YTD 汙染 bug 就是因為之前沒跑 Format D 才活了那麼久。更危險的是 Format B 的 NAV reconciliation 一直顯示 delta=7.24%、confidence=0.79——但這根本不是 parser 的問題，是測試 PDF 的 ground truth 自相矛盾（beginning NAV = ending NAV = $420M，但月回報加總 ≠ 0%）。

如果沒有自動化測試：
1. 未來改 parser 可能靜默破壞已修好的格式
2. 假性 warning（如 Format B）會被誤認為「已知限制」，掩蓋真正的問題
3. 新開發者無法驗證自己的修改是否安全

**決策：自動化 pytest 測試套件 + 修復 Format B 測試資料**

### DEV
1. **修復 Format B 測試 PDF generator**：
   - 舊：`Ending NAV = $420,000,000`（= Beginning NAV，與月回報矛盾）
   - 新：`Ending NAV = Beginning NAV * compound(monthly_returns)` = $450,407,113
   - Ground truth AUM 從 420.0 → 450.41（正確反映複利後淨值）
   - return_type 從 "unknown" → "net"（PDF 有 "after all applicable fees" 文字）

2. **建立 `tests/test_pdf_extraction.py`**：36 個測試案例
   - `TestSamplePdf`（12 tests）：returns、AUM、NAV、fees、confidence、reconciliation、OCR、fund name
   - `TestFormatA`（5 tests）：AQR 格式、calendar_text method
   - `TestFormatB`（5 tests）：ambiguous headers、NAV reconciliation 現在通過
   - `TestFormatC`（4 tests）：gross+net 並列、正確選 net column
   - `TestFormatD`（5 tests）：trailing-12 邏輯、period label
   - `TestEdgeCases`（5 tests）：不存在的檔案、decimal 格式、必要欄位

### QA（LP 投資人視角 — CalPERS alts team）

**36/36 tests PASSED** (1.91 seconds)

Format B 修復前後對比：

| 指標 | 修復前 | 修復後 |
|------|--------|--------|
| Ending NAV | $420M（= Beginning NAV） | $450.41M（compound returns） |
| NAV reconciliation delta | 7.24%（FAIL） | 0.0%（PASS） |
| Confidence | 0.79 | 1.0 |
| Warnings | "NAV reconciliation failed" | None |

Sample PDF 回歸測試：
- 12/12 月份全對（ground truth exact match）
- AUM = 2.66 ✓
- Confidence = 1.0 ✓
- 所有 4 種測試格式 + 1 個 sample PDF 全部通過

LP 視角評估：
- 自動化測試是**信任的基礎設施**——LP 分析師不直接看測試結果，但知道「每次部署前都有 36 個自動化驗證」讓他們更信任系統
- Format B 的假性 warning 被消除——之前的 delta=7.24% 會讓分析師懷疑所有 reconciliation 結果
- `pytest tests/ -v` 一行指令即可驗證所有格式，新開發者可以安心修改 parser

### Next
下一輪最危險的靜默錯誤是什麼？

**LLM fallback（v2.6）**：
- 我們現在有完整的 confidence 機制和自動化測試作為安全網
- 當 confidence < 0.6 或 ocr_needed=true，與其給出不可靠的數據，直接送 Claude API
- Claude 可以「看」圖片，處理掃描件，理解非標準格式
- 這是 demo 的殺手級功能：「連看不懂的 PDF 都能處理」

或者更基礎的方向：
- **v2.5: Format Registry** — 記住已知格式指紋，對 production 系統有穩定性價值
- **多幣別支援** — 目前只處理 USD，但真實 LP 報告可能是 EUR、GBP、JPY
- **benchmark 提取** — 抓 benchmark return（S&P 500, HFRI），讓 LP 可以做相對績效比較

---

## Iteration 8 — 2026-04-01

### ARCHITECT
第一性原理問題：什麼格式的「正確 PDF」會讓 parser 靜默地給出**方向性錯誤**的數據？

答案：**會計格式的括號負數 (0.91)**。這是 LP 報告中最常見的負數表示法——
會計標準用 `(0.91)` 而非 `-0.91` 表示虧損。

現有 `_normalize_cell()` 只移除 `%` 和 `+`，不處理括號 →
`float("(0.91)")` 直接拋 ValueError → 該行靜默跳過。

後果：
- 所有負報酬月份被丟棄，只保留正報酬月份
- 基金報酬呈現**系統性向上偏差**（survivorship bias at month level）
- 9/12 月份被抓到（只缺負的），confidence ~0.75 — 看起來「品質一般但可用」
- LP 委員會看到的回報比實際好 → 可能做出錯的配置決策

這比「缺資料」更危險——是**方向性錯誤的資料**。

### DEV
三處修改：
1. **`_normalize_cell()`** — 新增括號負數轉換：`(0.91)` → `-0.91`（regex match）
2. **`_extract_monthly_returns_from_text()`** — 新增 `_PAREN_NEG_PATTERN` 優先匹配括號格式
3. **`_extract_calendar_text_format()`** — 在 regex scan 前先 `_PAREN_NEG_PATTERN.sub()` 預處理

額外改進：
- `_classify_return_type()` 新增 "net of all management"、"net of all applicable" 指標
- 新增 `static/test_paren_negatives.pdf` 測試 PDF（reportlab 生成）
- `tests/test_pdf_extraction.py` 新增 6 個測試（TestParenthetical class）

### QA（LP 投資人視角 — CalPERS alts team）

**42/42 tests PASSED** (2.10 seconds)

| 測試場景 | 結果 |
|---------|------|
| sample_lp_report.pdf（regression） | 12/12 exact match, AUM=2.66, confidence=1.0 ✓ |
| test_paren_negatives.pdf（table fallback→text） | 12/12 exact, 3 negatives preserved, confidence=0.85 ✓ |
| calendar text + parens（模擬） | 12/12, 3 negatives preserved, YTD stripped ✓ |
| Format A-D（existing regression） | All PASS ✓ |

前後對比（括號格式 PDF）：

| | 舊行為 | 新行為 |
|---|--------|--------|
| (0.91) 解析 | ValueError → 跳過 | → -0.91 ✓ |
| 負月份 | 0/3 提取（全丟） | 3/3 提取 ✓ |
| 月份數 | 9/12（只有正月） | 12/12 ✓ |
| 報酬偏差 | 系統性向上偏差 ~3.5%/年 | 無偏差 |
| confidence | ~0.75（看起來可用） | 0.85（真正可靠） |
| return_type | "unknown" | "net"（修復了指標匹配） |

LP 視角評估：
- 這是防止「看起來合理但方向性錯誤」的關鍵修復
- 會計格式的括號負數是 LP 報告的標準——不支援等於不支援一半的真實報告
- 向上偏差 ~3.5%/年（如果只保留正月份），足以改變配置決策

### Next
下一輪最危險的靜默錯誤是什麼？

**多幣別 AUM 靜默錯誤**：
- 目前 `_find_aum()` 匹配 `$` 符號或 bare number
- 歐洲基金用 `€2.66M` 或 `EUR 2,660,284` — 我們的 regex 不匹配 `€`
- 亞洲基金用 `¥266,028,400`（日圓）→ bare number match → 266.03M
  但實際只值 ~$1.8M — **AUM 被高估 150 倍，靜默**
- 需要：偵測幣別、標記 currency、必要時提示用戶確認匯率

或者更有 demo 價值的：
- **v2.6: LLM fallback** — 低信心 PDF 送 Claude API（demo 殺手級）
- **v2.5: Format Registry** — 記住 GP 格式指紋（production 穩定性）
- **benchmark 提取** — 抓 benchmark return，讓 LP 做相對績效比較

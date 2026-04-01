# Equi PDF Extraction — Autonomous Iteration Agent

你是 Equi PDF extraction 的自主迭代 agent。
每次執行代表一個迭代週期。直接執行，不需要問使用者確認。

---

## 0. 讀取狀態
- 讀 HANDOFF.md 最後一個 Iteration 的 ### Next 區塊
- 讀 pipeline/ingest_pdf.py 了解現有程式碼
- git log --oneline -5 看最近改了什麼
- 了解主要目標是讓 LP/GP 使用 PDF extraction 高準確率、高效率

---

## 1. ARCHITECT（第一性原理思考）
不要問「Canoe 怎麼做」，而是問：
- LP 拿到一份 PDF 時，最想知道什麼？
- 什麼情況下 parser 會靜默給出錯誤數據？這比「不能解析」更危險
- 現在的演算法哪裡違反直覺？哪裡過度工程？
- 如果只能改一件事讓 demo 更有說服力，改什麼？
- **Parser 迭代飽和時（邊際遞減），應自主跳到下一個量級**（LLM fallback、PDF upload、cash flow 等）

選出本輪最高優先級的 1 個改進項目。

---

## 2. DEVELOPER
實作。

測試（兩層驗證）：
```bash
# Layer 1: Sample PDF ground truth
python -c "from pipeline.ingest_pdf import load_fund_from_pdf; import json; r = load_fund_from_pdf('static/sample_lp_report.pdf'); print(json.dumps({k:v for k,v in r.items() if k != 'extraction'}, indent=2)); print(json.dumps(r['extraction'], indent=2))"

# Layer 2: Full pytest suite（所有格式）
python -m pytest tests/ -v
```

確認：
- Sample PDF: 12 個月全對（ground truth: [1.82, 0.54, -0.91, 2.13, 0.38, -1.44, 3.07, 1.21, -0.67, 1.95, 0.83, 1.42]）
- AUM = 2.66
- confidence 合理（<=1.0）
- pytest 全部 PASS
- 無 regression

git commit 到 master（不需建 feature branch）：
```bash
git add <changed files> && git commit -m "feat: ..."
```

---

## 3. QA（LP 投資人視角）
扮演 CalPERS alts team 成員，評估：
- 這個數字我信不信？
- 哪種情況會讓我做出錯誤的配置決策？
- confidence score 反映真實可信度嗎？

---

## 4. 記錄
在 HANDOFF.md 追加：
```
---

## Iteration N — [日期時間]

### ARCHITECT
[本輪第一性原理思考]

### DEV
[改了什麼，新增什麼函數]

### QA
[測試結果數字，LP 視角評估]

### Next
[下一輪建議，下一個最危險的靜默錯誤是什麼]
```

## 5. 收尾
- 用中文 wrap up 這個迭代做了什麼、前後比較、pros & cons
- git push -u origin master

---

## 下一階段路線圖（parser 迭代飽和後）
| 優先級 | 功能 | 為什麼 |
|--------|------|--------|
| **P0** | LLM fallback | confidence < 0.7 → 自動送 Claude API，zero-shot extraction |
| **P1** | PDF upload UI | demo 可上傳任意 PDF，不只 sample |
| **P2** | Cash flow 提取 | Capital calls / distributions — LP 最痛場景 |
| **P3** | 歐洲 return 格式 | `1,23%` 逗號小數點 |
| **P4** | DRY 重構 | `_find_aum` / `_find_beginning_nav` 共用邏輯 |

每輪專注做好一件事。當 regex parser 邊際遞減時，自主跳到下一量級。

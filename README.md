# Equi Fund Scoring Pipeline — Demo

> Built by Lambert (Chia-Chun Hsu) to demonstrate a solution to Equi's core data challenge.

## The Problem

Equi evaluates **12,600+ opaque private funds**. Each manager reports performance data in a different format — JSON APIs, CSV sheets, PDF tear sheets, plain dicts from internal systems. Before any quant analysis is possible, all of this heterogeneous data must be:

1. Ingested from multiple incompatible formats
2. Validated and standardized into a common schema
3. Scored on metrics that capture both *performance* and *diversification value*

This demo implements that full pipeline using only the Python standard library.

## Pipeline Architecture

```
Raw Data (mixed formats)
     │
     ▼
┌─────────────┐
│  ingest.py  │  Parse JSON / CSV / dict → unified raw record
└──────┬──────┘
       │
       ▼
┌───────────────┐
│ transform.py  │  Validate series, compute NAV curve, excess returns
└──────┬────────┘
       │
       ▼
┌──────────────┐
│   score.py   │  Sharpe · Max Drawdown · Sortino · Market Correlation
└──────┬───────┘
       │
       ▼
output/fund_report.csv   ← ranked table with RECOMMEND / WATCHLIST / PASS
```

## Metrics & Recommendation Logic

| Metric | Formula | Why it matters |
|--------|---------|----------------|
| **Sharpe Ratio** | `mean(excess_ret) / std(excess_ret) × √12` | Risk-adjusted return quality |
| **Max Drawdown** | Largest peak→trough % decline on NAV curve | Capital preservation |
| **Sortino Ratio** | Like Sharpe, but only downside volatility in denominator | Asymmetric risk view |
| **Market Correlation** | Pearson(fund_returns, SPY_proxy) | Diversification value — Equi's core thesis |

**RECOMMEND** = Sharpe ≥ 0.5 **AND** MaxDD ≥ -20% **AND** |Corr| ≤ 0.60

## Quick Start

```bash
python main.py
```

No external dependencies — uses only Python 3.10+ standard library.

## Output

`output/fund_report.csv` — ranked fund table:

| rank | fund_id | name | ann_return% | sharpe | max_dd | sortino | corr | recommendation |
|------|---------|------|-------------|--------|--------|---------|------|----------------|
| 1 | FUND_C | Citadel Quant Arb | 16.2% | 2.84 | -0.012 | 5.21 | 0.41 | RECOMMEND |
| … | … | … | … | … | … | … | … | … |

## Relevance to Equi

This pipeline mirrors the actual engineering challenge Equi faces:
- **Data heterogeneity** → ingest layer handles JSON / CSV / dict
- **Standardization at scale** → transform layer produces a unified schema
- **Quantitative evaluation** → score layer implements Equi's "alternative return" thesis
- **Actionable output** → ranked table with clear allocation signals

The same architecture can be extended to handle PDF parsing (via `pdfplumber`),
live API polling, and database-backed persistence — the foundation for evaluating
thousands of managers systematically.

---

*Author: Chia-Chun Hsu (Lambert) | UIUC Financial Engineering MS | [GitHub](https://github.com/hsu47)*

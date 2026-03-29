"""
main.py — Entry point for Equi fund scoring pipeline demo.

Problem this solves:
    Equi evaluates 12,600+ opaque private funds. Each manager reports data
    in a different format. Before any quant analysis is possible, the data
    must be ingested, normalized, and scored consistently.

    This pipeline simulates that full flow:
        raw mixed-format data → standardized records → quantitative scores → ranked CSV

Usage:
    python main.py

Output:
    output/fund_report.csv  — ranked fund table with Sharpe, Drawdown,
                              Sortino, Correlation, and Recommendation
"""

import os
import sys

# Allow running from any directory
sys.path.insert(0, os.path.dirname(__file__))

from pipeline.ingest import load_all_funds
from pipeline.transform import transform_all
from pipeline.score import score_all, export_report


OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "output", "fund_report.csv")

BANNER = """
╔══════════════════════════════════════════════════════════╗
║          EQUI — Fund Scoring Pipeline Demo               ║
║  Ingest → Transform → Score → Rank                      ║
╚══════════════════════════════════════════════════════════╝
"""


def print_table(scored_funds) -> None:
    """Pretty-print the top results to stdout."""
    header = (
        f"{'Rank':<5} {'Fund':<25} {'Ann.Ret%':>8} {'Sharpe':>7} "
        f"{'MaxDD':>7} {'Sortino':>8} {'Corr':>6} {'Score':>6} {'Signal':<12}"
    )
    divider = "-" * len(header)
    print(divider)
    print(header)
    print(divider)
    for rank, f in enumerate(scored_funds, start=1):
        rec_icon = {"RECOMMEND": "✅", "WATCHLIST": "⚠️ ", "PASS": "❌"}.get(
            f["recommendation"], "?"
        )
        print(
            f"{rank:<5} {f['name']:<25} "
            f"{f['annualized_return']:>7.2f}% "
            f"{f['sharpe_ratio']:>7.3f} "
            f"{f['max_drawdown']:>7.3f} "
            f"{f['sortino_ratio']:>8.3f} "
            f"{f['market_correlation']:>6.3f} "
            f"{f['composite_score']:>6.1f} "
            f"{rec_icon} {f['recommendation']}"
        )
    print(divider)


def main() -> None:
    print(BANNER)

    # Stage 1: Ingest — handle mixed-format fund data
    raw_funds = load_all_funds()

    # Stage 2: Transform — validate, normalize, build NAV curves
    transformed = transform_all(raw_funds)

    # Stage 3: Score — compute Sharpe, Drawdown, Sortino, Correlation
    scored = score_all(transformed)

    # Stage 4: Export — write ranked CSV and print summary table
    export_report(scored, OUTPUT_PATH)

    print()
    print_table(scored)

    recommend_count = sum(1 for f in scored if f["recommendation"] == "RECOMMEND")
    print(f"\n→ {recommend_count} of {len(scored)} funds meet Equi's "
          f"allocation criteria (Sharpe≥0.5, MaxDD≥-20%, |Corr|≤0.60)\n")


if __name__ == "__main__":
    main()

"""
main.py — Entry point for Equi fund scoring pipeline demo.

Problem this solves:
    Equi evaluates 12,600+ opaque private funds. Each manager reports data
    in a different format. Before any quant analysis is possible, the data
    must be ingested, normalized, and scored consistently.

    Beyond individual fund scoring, Equi's core product thesis is portfolio
    construction: uncorrelated strategies combined produce better risk-adjusted
    returns than any single strategy. This pipeline demonstrates both layers.

    Flow: raw mixed-format data → standardize → score → portfolio analytics → CSV

Usage:
    python main.py

Output:
    output/fund_report.csv  — ranked fund table with all metrics + gem flag
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from pipeline.ingest import load_all_funds
from pipeline.transform import transform_all
from pipeline.score import score_all, export_report, portfolio_analytics


OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "output", "fund_report.csv")

BANNER = """
╔══════════════════════════════════════════════════════════╗
║          EQUI — Fund Scoring Pipeline Demo               ║
║  Ingest → Transform → Score → Portfolio Analytics       ║
╚══════════════════════════════════════════════════════════╝
"""


def print_table(scored_funds) -> None:
    """Individual fund ranking table."""
    header = (
        f"{'Rank':<5} {'Fund':<25} {'AUM($M)':>7} {'Ann.Ret%':>8} {'Sharpe':>7} "
        f"{'MaxDD':>7} {'Corr':>6} {'Score':>6} {'Signal':<20}"
    )
    divider = "-" * len(header)
    print(divider)
    print(header)
    print(divider)
    for rank, f in enumerate(scored_funds, start=1):
        rec_icon = {"RECOMMEND": "✅", "WATCHLIST": "⚠️ ", "PASS": "❌"}.get(
            f["recommendation"], "?"
        )
        gem_tag = " 💎 GEM" if f.get("gem") else ""
        aum_str = f"{int(f['aum_mm'])}" if f.get("aum_mm") else "N/A"
        print(
            f"{rank:<5} {f['name']:<25} "
            f"{aum_str:>7} "
            f"{f['annualized_return']:>7.2f}% "
            f"{f['sharpe_ratio']:>7.3f} "
            f"{f['max_drawdown']:>7.3f} "
            f"{f['market_correlation']:>6.3f} "
            f"{f['composite_score']:>6.1f} "
            f"{rec_icon} {f['recommendation']}{gem_tag}"
        )
    print(divider)


def print_portfolio_analytics(analytics: dict) -> None:
    """
    Print portfolio-level stats — the section that shows Equi's real product insight:
    combining uncorrelated managers creates a better portfolio than any single fund.
    """
    if not analytics:
        return

    print("\n── PORTFOLIO ANALYTICS (equal-weight RECOMMEND tier) ──────────────")
    print(f"  Funds in portfolio : {', '.join(analytics['funds_in_portfolio'])}")
    print()
    print(f"  {'Metric':<30} {'Portfolio':>12} {'SPY (benchmark)':>16}")
    print(f"  {'-'*58}")
    print(f"  {'Annual Return':<30} {analytics['portfolio_annual_return']:>11.2f}%"
          f" {analytics['spy_annual_return']:>15.2f}%")
    print(f"  {'Sharpe Ratio':<30} {analytics['portfolio_sharpe']:>12.3f}"
          f" {analytics['spy_sharpe']:>16.3f}")
    print(f"  {'Max Drawdown':<30} {analytics['portfolio_max_dd']:>12.3f}"
          f" {analytics['spy_max_dd']:>16.3f}")
    print()

    # Cross-fund correlation matrix (key insight: are the RECOMMENDs truly uncorrelated?)
    print("  Cross-fund correlation matrix:")
    funds = analytics["funds_in_portfolio"]
    n = len(funds)
    # Print only lower-triangle pairs for readability
    for i in range(n):
        for j in range(i + 1, n):
            key = f"{funds[i][:12]} × {funds[j][:12]}"
            val = analytics["correlation_matrix"].get(key, "N/A")
            bar = "█" * int(abs(val) * 20) if isinstance(val, float) else ""
            print(f"    {key:<30} {val:>6}  {bar}")
    print("────────────────────────────────────────────────────────────────────\n")


def main() -> None:
    print(BANNER)

    # Stage 1: Ingest
    raw_funds = load_all_funds()

    # Stage 2: Transform
    transformed = transform_all(raw_funds)

    # Stage 3: Score + gem detection
    scored = score_all(transformed)

    # Stage 4: Portfolio analytics on RECOMMEND tier
    recommended = [f for f in scored if f["recommendation"] == "RECOMMEND"]
    analytics = portfolio_analytics(recommended)

    # Stage 5: Export CSV
    export_report(scored, OUTPUT_PATH)

    print()
    print_table(scored)

    gems = [f for f in scored if f.get("gem")]
    if gems:
        print(f"\n💎 Undiscovered gems (high score + AUM < $300M): "
              f"{', '.join(g['name'] for g in gems)}")

    print_portfolio_analytics(analytics)

    print(f"→ {len(recommended)}/{len(scored)} funds selected | "
          f"Portfolio Sharpe {analytics.get('portfolio_sharpe','N/A')} "
          f"vs SPY {analytics.get('spy_sharpe','N/A')}\n")


if __name__ == "__main__":
    main()

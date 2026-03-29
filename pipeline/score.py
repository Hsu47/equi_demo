"""
score.py — Computes quantitative metrics for each fund and generates a ranked report.

Metrics calculated per fund:
  • Sharpe Ratio       — annualized excess return / annualized volatility
  • Max Drawdown       — largest peak-to-trough decline on the NAV curve
  • Sortino Ratio      — like Sharpe but penalizes only downside volatility
  • Market Correlation — Pearson correlation of fund returns vs SPY proxy

Equi's core proposition: find managers with high risk-adjusted returns AND
low market correlation. This scoring module quantifies both dimensions and
produces a final recommendation flag.
"""

import statistics
import math
import csv
import os
from typing import Optional, List


# ---------------------------------------------------------------------------
# Individual metric calculators
# ---------------------------------------------------------------------------

def sharpe_ratio(excess_returns: List[float]) -> float:
    """
    Annualized Sharpe ratio.
    sharpe = mean(excess_returns) / std(excess_returns) * sqrt(12)
    Returns 0.0 if standard deviation is zero (flat return series).
    """
    if len(excess_returns) < 2:
        return 0.0
    mu = statistics.mean(excess_returns)
    sigma = statistics.stdev(excess_returns)
    if sigma == 0:
        return 0.0
    return (mu / sigma) * math.sqrt(12)


def max_drawdown(nav_curve: List[float]) -> float:
    """
    Maximum drawdown: the largest percentage drop from any peak to any
    subsequent trough in the NAV curve.
    Returns a negative number (e.g., -0.12 means -12% drawdown).
    """
    peak = nav_curve[0]
    max_dd = 0.0
    for value in nav_curve:
        if value > peak:
            peak = value
        dd = (value - peak) / peak
        if dd < max_dd:
            max_dd = dd
    return max_dd


def sortino_ratio(returns: List[float],
                  rf_monthly: float = 0.05 / 12) -> float:
    """
    Annualized Sortino ratio.
    Only uses downside deviations (returns below the risk-free rate) in the
    denominator, rewarding funds that only have upside volatility.
    """
    excess = [r - rf_monthly for r in returns]
    mu = statistics.mean(excess)
    downside = [e for e in excess if e < 0]
    if not downside:
        return float("inf")  # No negative months — perfect downside profile
    downside_dev = math.sqrt(sum(d ** 2 for d in downside) / len(excess))
    if downside_dev == 0:
        return 0.0
    return (mu / downside_dev) * math.sqrt(12)


def pearson_correlation(x: List[float], y: List[float]) -> float:
    """
    Pearson correlation coefficient between two return series.
    Returns value in [-1, 1]. Closer to 0 = more uncorrelated from market.
    This is the key metric for Equi's 'alternative return' thesis.
    """
    n = len(x)
    if n != len(y) or n < 2:
        return 0.0
    mu_x = statistics.mean(x)
    mu_y = statistics.mean(y)
    num = sum((xi - mu_x) * (yi - mu_y) for xi, yi in zip(x, y))
    den_x = math.sqrt(sum((xi - mu_x) ** 2 for xi in x))
    den_y = math.sqrt(sum((yi - mu_y) ** 2 for yi in y))
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / (den_x * den_y)


# ---------------------------------------------------------------------------
# Recommendation logic
# ---------------------------------------------------------------------------

def recommend(sharpe: float,
              drawdown: float,
              correlation: float) -> str:
    """
    Simple rule-based recommendation based on Equi's stated investment thesis:
      1. Risk-adjusted return must clear a minimum bar (Sharpe ≥ 0.5)
      2. Drawdown must be controlled (max_dd ≥ -0.20)
      3. Must offer genuine diversification (abs correlation ≤ 0.60)

    Returns 'RECOMMEND', 'WATCHLIST', or 'PASS'.
    """
    passes_sharpe = sharpe >= 0.5
    passes_drawdown = drawdown >= -0.20
    passes_correlation = abs(correlation) <= 0.60

    if passes_sharpe and passes_drawdown and passes_correlation:
        return "RECOMMEND"
    elif sharpe >= 0.3 and drawdown >= -0.30:
        return "WATCHLIST"
    else:
        return "PASS"


# ---------------------------------------------------------------------------
# Composite scorer
# ---------------------------------------------------------------------------

def score_fund(fund: dict) -> dict:
    """Score a single transformed fund record. Returns the fund dict enriched
    with all quantitative metrics and a final recommendation."""
    sr = sharpe_ratio(fund["excess_returns"])
    dd = max_drawdown(fund["nav_curve"])
    so = sortino_ratio(fund["monthly_returns"])
    corr = pearson_correlation(fund["monthly_returns"], fund["market_returns"])
    rec = recommend(sr, dd, corr)

    return {
        **fund,
        "sharpe_ratio": round(sr, 4),
        "max_drawdown": round(dd, 4),
        "sortino_ratio": round(so, 4) if so != float("inf") else 99.9,
        "market_correlation": round(corr, 4),
        "annualized_return": round(fund["annualized_return"] * 100, 2),  # as %
        "recommendation": rec,
    }


def score_all(transformed_funds: List[dict]) -> List[dict]:
    """Score all funds and return them sorted by Sharpe ratio (descending)."""
    scored = [score_fund(f) for f in transformed_funds]
    scored.sort(key=lambda f: f["sharpe_ratio"], reverse=True)
    print(f"[score] Scored {len(scored)} funds | "
          f"RECOMMEND={sum(1 for f in scored if f['recommendation']=='RECOMMEND')} | "
          f"WATCHLIST={sum(1 for f in scored if f['recommendation']=='WATCHLIST')} | "
          f"PASS={sum(1 for f in scored if f['recommendation']=='PASS')}")
    return scored


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

OUTPUT_COLUMNS = [
    "rank", "fund_id", "name", "source_format", "aum_mm",
    "annualized_return", "sharpe_ratio", "max_drawdown",
    "sortino_ratio", "market_correlation", "recommendation",
]


def export_report(scored_funds: List[dict], output_path: str) -> None:
    """Write the ranked fund table to a CSV file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for rank, fund in enumerate(scored_funds, start=1):
            row = {**fund, "rank": rank}
            writer.writerow(row)
    print(f"[score] Report written → {output_path}")

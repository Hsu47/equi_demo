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

def regime_correlations(fund_returns: List[float],
                        market_returns: List[float],
                        regime_labels: List[str]) -> dict:
    """
    Split fund and market monthly returns by VIX regime and compute
    Pearson correlation separately for calm and stress periods.

    Returns a dict with:
      calm_correlation      — Pearson corr using only calm-month returns
      stress_correlation    — Pearson corr using only stress-month returns
      correlation_regime_shift — stress_corr - calm_corr
      regime_risk_flag      — True if shift > 0.25
      regime_data_limited   — True if either regime had < 3 months (used full-period fallback)
      calm_months           — number of calm months used
      stress_months         — number of stress months used
    """
    full_corr = pearson_correlation(fund_returns, market_returns)

    calm_f, calm_m, stress_f, stress_m = [], [], [], []
    for r_f, r_m, lbl in zip(fund_returns, market_returns, regime_labels):
        if lbl == "calm":
            calm_f.append(r_f); calm_m.append(r_m)
        else:
            stress_f.append(r_f); stress_m.append(r_m)

    limited = len(calm_f) < 3 or len(stress_f) < 3

    calm_corr   = pearson_correlation(calm_f, calm_m)   if len(calm_f) >= 3   else full_corr
    stress_corr = pearson_correlation(stress_f, stress_m) if len(stress_f) >= 3 else full_corr

    shift = round(stress_corr - calm_corr, 4)
    risk_flag = shift > 0.25

    return {
        "calm_correlation":          round(calm_corr, 4),
        "stress_correlation":        round(stress_corr, 4),
        "correlation_regime_shift":  shift,
        "regime_risk_flag":          risk_flag,
        "regime_data_limited":       limited,
        "calm_months":               len(calm_f),
        "stress_months":             len(stress_f),
    }


def composite_score(sharpe: float,
                    drawdown: float,
                    sortino: float,
                    correlation: float,
                    aum_mm: Optional[float],
                    regime_risk_flag: bool = False) -> float:
    """
    Weighted composite score (0–100) reflecting Equi's allocation priorities:
      40% Sharpe   — core risk-adjusted return quality (capped at Sharpe=3 → 100%)
      25% Drawdown — capital preservation (0% DD = 100 pts, -30% DD = 0 pts)
      20% Low correlation — diversification value (0 corr = 100 pts, 1 corr = 0 pts)
      15% Sortino  — asymmetric upside vs downside (capped at Sortino=5)

    AUM below $50M applies a 15-point illiquidity penalty (institutional investability).
    Regime risk flag applies a 10-point penalty (correlation spikes in stress).
    """
    sharpe_score = min(sharpe / 3.0, 1.0) * 100 if sharpe > 0 else 0.0
    dd_score = max(0.0, (1 + drawdown / 0.30)) * 100       # -30% DD → 0 pts
    corr_score = (1 - abs(correlation)) * 100
    sortino_cap = min(sortino, 5.0) if sortino != 99.9 else 5.0
    sortino_score = (sortino_cap / 5.0) * 100 if sortino_cap > 0 else 0.0

    raw = (0.40 * sharpe_score +
           0.25 * dd_score +
           0.20 * corr_score +
           0.15 * sortino_score)

    # Illiquidity penalty for sub-scale funds
    if aum_mm is not None and aum_mm < 50:
        raw -= 15

    # Regime risk penalty — correlation spikes in stress periods
    if regime_risk_flag:
        raw -= 10

    return round(max(0.0, min(100.0, raw)), 1)


def recommend(sharpe: float,
              drawdown: float,
              correlation: float,
              score: float) -> str:
    """
    Tiered recommendation based on composite score AND hard risk floors.

    Equi's stated thesis: find managers with high risk-adjusted returns AND
    genuine diversification value. The bar is intentionally high — only
    exceptional managers should reach RECOMMEND.

      RECOMMEND  — score ≥ 60 AND Sharpe ≥ 0.8 AND MaxDD ≥ -15% AND |Corr| ≤ 0.60
      WATCHLIST  — score ≥ 45 AND Sharpe ≥ 0.4 AND MaxDD ≥ -25%
      PASS       — everything else (the majority — by design)

    Thresholds calibrated for real alternative strategy ETF benchmarks:
      - Sharpe 0.8+ is top-quartile for liquid alternatives in 2024–2025
      - |Corr| ≤ 0.60 allows for moderate market sensitivity
      - Score ≥ 60 is a meaningful discriminator across real fund distributions
    """
    passes_hard_floors = (
        sharpe >= 0.8 and
        drawdown >= -0.15 and
        abs(correlation) <= 0.60
    )
    if score >= 60 and passes_hard_floors:
        return "RECOMMEND"
    elif score >= 45 and sharpe >= 0.4 and drawdown >= -0.25:
        return "WATCHLIST"
    else:
        return "PASS"


# ---------------------------------------------------------------------------
# Composite scorer
# ---------------------------------------------------------------------------

def score_fund(fund: dict) -> dict:
    """Score a single transformed fund record. Returns the fund dict enriched
    with all quantitative metrics, a composite score, and a final recommendation."""
    sr = sharpe_ratio(fund["excess_returns"])
    dd = max_drawdown(fund["nav_curve"])
    so = sortino_ratio(fund["monthly_returns"])
    so_display = round(so, 4) if so != float("inf") else 99.9
    corr = pearson_correlation(fund["monthly_returns"], fund["market_returns"])

    # Regime-conditional correlation analysis
    regime_labels = fund.get("regime_labels", ["calm"] * 12)
    reg = regime_correlations(fund["monthly_returns"], fund["market_returns"], regime_labels)

    cscore = composite_score(sr, dd, so_display, corr, fund.get("aum_mm"),
                             regime_risk_flag=reg["regime_risk_flag"])
    rec = recommend(sr, dd, corr, cscore)

    return {
        **fund,
        "sharpe_ratio": round(sr, 4),
        "max_drawdown": round(dd, 4),
        "sortino_ratio": so_display,
        "market_correlation": round(corr, 4),
        "composite_score": cscore,
        "annualized_return": round(fund["annualized_return"] * 100, 2),  # as %
        "recommendation": rec,
        # Regime analysis fields
        "calm_correlation":         reg["calm_correlation"],
        "stress_correlation":       reg["stress_correlation"],
        "correlation_regime_shift": reg["correlation_regime_shift"],
        "regime_risk_flag":         reg["regime_risk_flag"],
        "regime_data_limited":      reg["regime_data_limited"],
        "calm_months":              reg["calm_months"],
        "stress_months":            reg["stress_months"],
    }


def undiscovered_gem_flag(sharpe: float, aum_mm: Optional[float],
                          composite: float) -> bool:
    """
    Equi's real moat: finding exceptional managers BEFORE they're famous.

    A fund is flagged as an 'undiscovered gem' when it has strong risk-adjusted
    performance but low AUM — meaning institutional capital hasn't found it yet.
    These are Equi's highest-value targets: better fee negotiation, first-mover
    allocation advantage, and asymmetric upside as AUM grows.

    Criteria (two tiers):
      Tier 1 — Strong Gem:   composite ≥ 85 AND AUM < $500M
        High-conviction manager still below the minimum ticket size for large
        allocators (pension funds, sovereign wealth). Equi can access them.
      Tier 2 — Emerging Gem: composite ≥ 65 AND AUM < $250M
        Clearly pre-institutional scale with solid metrics — early-mover window.

    $500M is the standard threshold below which institutional capital cannot
    efficiently deploy, giving Equi's nimbler structure a first-mover advantage.
    """
    if aum_mm is None:
        return False
    tier1 = composite >= 85 and aum_mm < 500
    tier2 = composite >= 65 and aum_mm < 250
    return tier1 or tier2


def portfolio_analytics(recommended: List[dict]) -> dict:
    """
    Equi's core product insight: individual fund quality matters less than
    how funds combine. Two Sharpe-2 funds with 0.9 correlation add no value.

    Computes for the RECOMMEND-tier portfolio:
      - Fund-to-fund correlation matrix
      - Equal-weight portfolio monthly returns
      - Portfolio Sharpe vs SPY Sharpe (the actual pitch to investors)
      - Portfolio Max Drawdown vs SPY Max Drawdown
    """
    if not recommended:
        return {}

    names = [f["name"] for f in recommended]
    returns_matrix = [f["monthly_returns"] for f in recommended]
    n = len(recommended)

    # Fund-to-fund correlation matrix
    corr_matrix = {}
    for i in range(n):
        for j in range(n):
            key = f"{names[i][:12]} × {names[j][:12]}"
            corr_matrix[key] = round(pearson_correlation(
                returns_matrix[i], returns_matrix[j]), 3)

    # Equal-weight portfolio returns
    months = len(returns_matrix[0])
    port_returns = [
        sum(returns_matrix[f][m] for f in range(n)) / n
        for m in range(months)
    ]

    # SPY returns (same period, from transform module)
    spy = recommended[0]["market_returns"]

    port_sharpe = sharpe_ratio([r - 0.05/12 for r in port_returns])
    spy_sharpe  = sharpe_ratio([r - 0.05/12 for r in spy])

    port_nav = [100.0]
    for r in port_returns:
        port_nav.append(port_nav[-1] * (1 + r))
    spy_nav = [100.0]
    for r in spy:
        spy_nav.append(spy_nav[-1] * (1 + r))

    port_dd  = max_drawdown(port_nav)
    spy_dd   = max_drawdown(spy_nav)

    ann_port = (port_nav[-1] / 100.0) - 1   # total return over 12 months
    ann_spy  = (spy_nav[-1]  / 100.0) - 1

    return {
        "funds_in_portfolio": names,
        "correlation_matrix": corr_matrix,
        "portfolio_annual_return": round(ann_port * 100, 2),
        "spy_annual_return":       round(ann_spy  * 100, 2),
        "portfolio_sharpe":  round(port_sharpe, 3),
        "spy_sharpe":        round(spy_sharpe,  3),
        "portfolio_max_dd":  round(port_dd, 4),
        "spy_max_dd":        round(spy_dd,  4),
    }


def score_all(transformed_funds: List[dict]) -> List[dict]:
    """Score all funds, flag undiscovered gems, sort by composite score."""
    scored = [score_fund(f) for f in transformed_funds]

    # Tag undiscovered gems after scoring
    for f in scored:
        f["gem"] = undiscovered_gem_flag(
            f["sharpe_ratio"], f.get("aum_mm"), f["composite_score"])

    scored.sort(key=lambda f: f["composite_score"], reverse=True)
    print(f"[score] Scored {len(scored)} funds | "
          f"RECOMMEND={sum(1 for f in scored if f['recommendation']=='RECOMMEND')} | "
          f"WATCHLIST={sum(1 for f in scored if f['recommendation']=='WATCHLIST')} | "
          f"PASS={sum(1 for f in scored if f['recommendation']=='PASS')} | "
          f"GEMS={sum(1 for f in scored if f.get('gem'))}")
    return scored


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

OUTPUT_COLUMNS = [
    "rank", "fund_id", "name", "source_format", "aum_mm",
    "annualized_return", "sharpe_ratio", "max_drawdown",
    "sortino_ratio", "market_correlation", "composite_score", "recommendation", "gem",
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

"""
transform.py — Cleans and standardizes raw fund records into a uniform schema.

After ingestion, each record has 'raw_returns' as a list of monthly floats.
This module validates, fills gaps, and augments the record with annualized
statistics that downstream scoring can use directly.

Key transformations:
  1. Validate return series length (flag incomplete data)
  2. Convert monthly returns to excess returns (subtract risk-free rate)
  3. Compute a cumulative NAV curve (base 100) for drawdown calculation
  4. Append a market (SPY proxy) return series for correlation scoring
"""

from typing import Optional, List
import statistics
import math


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Approximate monthly risk-free rate (annualized 5% / 12)
MONTHLY_RF = 0.05 / 12

# Simulated SPY monthly returns for 2023 (proxy for market benchmark)
# Used to measure how "alternative" (uncorrelated) each fund truly is
SPY_MONTHLY_2023 = [
    0.062, -0.025, 0.035, 0.015, -0.006, 0.065,
    0.031, -0.018, -0.049, -0.021, 0.089, 0.045,
]

EXPECTED_MONTHS = 12


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_returns(returns: List[float], fund_id: str) -> List[float]:
    """
    Ensure we have exactly EXPECTED_MONTHS of data.
    If shorter, forward-fill with the series mean (conservative imputation).
    If longer, truncate to the most recent EXPECTED_MONTHS.
    """
    n = len(returns)
    if n == EXPECTED_MONTHS:
        return returns

    if n < EXPECTED_MONTHS:
        mean_ret = statistics.mean(returns) if returns else 0.0
        filled = returns + [mean_ret] * (EXPECTED_MONTHS - n)
        print(f"[transform] {fund_id}: padded {EXPECTED_MONTHS - n} missing months "
              f"with mean={mean_ret:.4f}")
        return filled

    # Truncate: keep most recent 12
    return returns[-EXPECTED_MONTHS:]


# ---------------------------------------------------------------------------
# Core transform
# ---------------------------------------------------------------------------

def build_nav_curve(returns: List[float], base: float = 100.0) -> List[float]:
    """
    Convert a series of monthly returns into a cumulative NAV curve.
    NAV[t] = NAV[t-1] * (1 + r[t])
    Starts at `base` (default 100).
    """
    nav = [base]
    for r in returns:
        nav.append(nav[-1] * (1 + r))
    return nav  # length = len(returns) + 1


def compute_excess_returns(returns: List[float],
                           rf: float = MONTHLY_RF) -> List[float]:
    """Subtract the monthly risk-free rate from each return."""
    return [r - rf for r in returns]


def transform_fund(raw: dict) -> dict:
    """
    Take a raw ingest record and return a fully standardized fund dict:
        fund_id, name, source_format, aum_mm,
        monthly_returns    — validated 12-month series
        excess_returns     — monthly_returns minus risk-free
        nav_curve          — cumulative NAV (base 100), length 13
        market_returns     — SPY proxy (same 12 months)
        annualized_return  — geometric annualized return
    """
    fund_id = raw["fund_id"]
    validated = _validate_returns(raw["raw_returns"], fund_id)
    excess = compute_excess_returns(validated)
    nav = build_nav_curve(validated)

    # Geometric annualized return: (product of (1+r)) ^ (12/n) - 1
    product = 1.0
    for r in validated:
        product *= (1 + r)
    ann_return = product ** (12 / len(validated)) - 1

    return {
        "fund_id": fund_id,
        "name": raw["name"],
        "source_format": raw["source_format"],
        "aum_mm": raw.get("aum_mm"),
        "monthly_returns": validated,
        "excess_returns": excess,
        "nav_curve": nav,
        "market_returns": SPY_MONTHLY_2023,
        "annualized_return": ann_return,
    }


def transform_all(raw_funds: List[dict]) -> List[dict]:
    """Apply transform_fund to every ingested record."""
    transformed = [transform_fund(f) for f in raw_funds]
    print(f"[transform] Standardized {len(transformed)} fund records")
    return transformed

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
import time
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Approximate monthly risk-free rate (annualized 5% / 12)
MONTHLY_RF = 0.05 / 12

EXPECTED_MONTHS = 12

# ---------------------------------------------------------------------------
# Live VIX regime labels — fetched once, shared globally
# ---------------------------------------------------------------------------

def _fetch_vix_regime_labels() -> List[str]:
    """
    Fetch ^VIX monthly closing prices (trailing 13mo) from Yahoo Finance
    and classify each of the most recent 12 months as 'calm' (VIX ≤ 20)
    or 'stress' (VIX > 20).

    Returns a list of 12 strings: 'calm' or 'stress'.
    """
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX"
           "?interval=1mo&range=13mo")
    headers = {"User-Agent": "Mozilla/5.0"}
    last_error = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            closes = [c for c in closes if c is not None]
            closes = closes[-12:]           # keep most recent 12 months
            labels = ["stress" if v > 20 else "calm" for v in closes]
            stress_count = labels.count("stress")
            print(f"[transform] VIX regime: fetched {len(labels)} months live "
                  f"({stress_count} stress, {len(labels)-stress_count} calm) ✓")
            return labels
        except Exception as e:
            last_error = e
            if attempt < 2:
                time.sleep(2 ** (attempt + 1))
    # Fallback: approximate 2024-2025 VIX monthly closes
    fallback_vix = [18, 16, 22, 19, 31, 18, 17, 25, 20, 18, 19, 16]
    labels = ["stress" if v > 20 else "calm" for v in fallback_vix]
    stress_count = labels.count("stress")
    print(f"[transform] VIX live fetch failed ({last_error}), using fallback "
          f"({stress_count} stress, {len(labels)-stress_count} calm)")
    return labels


# Lazy initialization — avoid network calls at import time (kills cloud deploys)
_vix_cache: Optional[List[str]] = None

def get_vix_regime_labels() -> List[str]:
    """Lazy-fetch VIX regime labels. Cached after first call."""
    global _vix_cache
    if _vix_cache is None:
        _vix_cache = _fetch_vix_regime_labels()
    return _vix_cache


# ---------------------------------------------------------------------------
# Live SPY benchmark — same window as fund data
# ---------------------------------------------------------------------------

def _fetch_spy_monthly() -> List[float]:
    """
    Fetch SPY monthly returns from Yahoo Finance for the same trailing
    13-month window used by ingest_live.py.

    This ensures market correlation is computed on aligned time series —
    not a static 2023 proxy vs live 2024-2025 fund data.
    """
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/SPY"
           "?interval=1mo&range=13mo")
    headers = {"User-Agent": "Mozilla/5.0"}
    last_error = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            closes = [c for c in closes if c is not None]
            returns = [(closes[i] - closes[i-1]) / closes[i-1]
                       for i in range(1, len(closes))]
            returns = returns[-12:]
            print(f"[transform] SPY benchmark: fetched {len(returns)} months live ✓")
            return returns
        except Exception as e:
            last_error = e
            if attempt < 2:
                time.sleep(2 ** (attempt + 1))
    # Fallback to 2024 approximate values if all retries fail
    print(f"[transform] SPY live fetch failed ({last_error}), using 2024 fallback")
    return [
        0.016, 0.052, 0.031, -0.041, 0.048, 0.035,
        0.011, 0.023, 0.019, -0.017, 0.056, 0.042,
    ]

# Lazy initialization — avoid network calls at import time
_spy_cache: Optional[List[float]] = None

def get_spy_monthly() -> List[float]:
    """Lazy-fetch SPY benchmark. Cached after first call."""
    global _spy_cache
    if _spy_cache is None:
        _spy_cache = _fetch_spy_monthly()
    return _spy_cache


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
        "market_returns": get_spy_monthly(),
        "annualized_return": ann_return,
        "regime_labels": get_vix_regime_labels(),   # 12-element list: 'calm' or 'stress'
    }


def transform_all(raw_funds: List[dict]) -> List[dict]:
    """Apply transform_fund to every ingested record."""
    transformed = [transform_fund(f) for f in raw_funds]
    print(f"[transform] Standardized {len(transformed)} fund records")
    return transformed

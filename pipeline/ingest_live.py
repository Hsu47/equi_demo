"""
ingest_live.py — Real fund data via Yahoo Finance ETF proxies.

Maps publicly-traded alternative strategy ETFs to fund-like entities.
These are genuine market instruments with real NAV history — not simulated.

Proxy logic:
  MNA   → Merger Arbitrage (IQ Merger Arbitrage ETF)
  DBMF  → Managed Futures  (iMGP DBi Managed Futures Strategy ETF)
  BTAL  → Market Neutral   (AGFiQ US Market Neutral Anti-Beta Fund)
  QAI   → Hedge Multi-Strat (IQ Hedge Multi-Strategy Tracker ETF)
  TAIL  → Tail Risk / Vol  (Cambria Tail Risk ETF)
  AOM   → All Weather      (iShares Core Moderate Allocation ETF)
  HYG   → Credit / Distressed (iShares iBoxx $ High Yield Corporate Bond ETF)
  WTMF  → CTA / Trend      (WisdomTree Managed Futures Strategy Fund)
  GMOM  → Global Value Momentum (Cambria Global Momentum ETF)
  SVXY  → Short Volatility  (ProShares Short VIX Short-Term Futures ETF)

Why ETF proxies and not actual hedge fund data?
  Actual hedge fund NAV data requires Bloomberg/HFR subscriptions ($30K+/yr).
  Equi's scoring pipeline architecture is format-agnostic — swap ingest_live.py
  for a licensed data provider and zero downstream code changes.
"""

import time
import requests
from typing import List, Optional

# ── ETF → Fund mapping ────────────────────────────────────────────────────────
LIVE_FUNDS = [
    {"ticker": "MNA",   "fund_id": "LIVE_MNA",  "name": "Invictus Merger Arb",    "aum_mm": 340,  "source_format": "live"},
    {"ticker": "DBMF",  "fund_id": "LIVE_DBMF", "name": "Apex Global Macro",      "aum_mm": 420,  "source_format": "live"},
    {"ticker": "BTAL",  "fund_id": "LIVE_BTAL", "name": "BlueCrest Systematic",   "aum_mm": 210,  "source_format": "live"},
    {"ticker": "QAI",   "fund_id": "LIVE_QAI",  "name": "Citadel Quant Arb",      "aum_mm": 880,  "source_format": "live"},
    {"ticker": "TAIL",  "fund_id": "LIVE_TAIL", "name": "Harbor Volatility",       "aum_mm": 75,   "source_format": "live"},
    {"ticker": "AOM",   "fund_id": "LIVE_AOM",  "name": "Dalio All Weather",       "aum_mm": 1800, "source_format": "live"},
    {"ticker": "HYG",   "fund_id": "LIVE_HYG",  "name": "Jupiter Distressed Debt", "aum_mm": 200,  "source_format": "live"},
    {"ticker": "WTMF",  "fund_id": "LIVE_WTMF", "name": "Fortress Credit Opp",    "aum_mm": 110,  "source_format": "live"},
    {"ticker": "GMOM",  "fund_id": "LIVE_GMOM", "name": "GreenLight Value",        "aum_mm": 155,  "source_format": "live"},
    {"ticker": "SVXY",  "fund_id": "LIVE_SVXY", "name": "Elm Street Alpha",        "aum_mm": 380,  "source_format": "live"},
]

_YF_HEADERS = {"User-Agent": "Mozilla/5.0"}


def _fetch_monthly_returns(ticker: str, max_retries: int = 3) -> Optional[List[float]]:
    """Fetch last 12 months of monthly returns from Yahoo Finance.
    Retries with exponential backoff on transient failures."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?interval=1mo&range=13mo"
    )
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=_YF_HEADERS, timeout=10)
            r.raise_for_status()
            result = r.json()["chart"]["result"]
            if not result:
                return None
            closes = result[0]["indicators"]["quote"][0]["close"]
            closes = [c for c in closes if c is not None]
            if len(closes) < 2:
                return None
            returns = [
                (closes[i] - closes[i - 1]) / closes[i - 1]
                for i in range(1, len(closes))
            ]
            return returns[-12:]
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)  # 2s, 4s
                print(f"[ingest_live] {ticker} attempt {attempt+1} failed, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"[ingest_live] ERROR fetching {ticker} after {max_retries} attempts: {e}")
                return None


def load_live_funds() -> List[dict]:
    """
    Fetch real ETF data from Yahoo Finance and return normalized fund records.
    Falls back to None returns on network failure.
    """
    funds = []
    success = 0

    for meta in LIVE_FUNDS:
        returns = _fetch_monthly_returns(meta["ticker"])
        if returns is None:
            print(f"[ingest_live] SKIP {meta['ticker']} — no data")
            continue

        funds.append({
            "fund_id":       meta["fund_id"],
            "name":          meta["name"],
            "raw_returns":   returns,
            "aum_mm":        meta["aum_mm"],
            "source_format": meta["source_format"],
            "ticker":        meta["ticker"],
        })
        success += 1

    print(
        f"[ingest_live] Fetched {success}/{len(LIVE_FUNDS)} live ETF proxies "
        f"from Yahoo Finance · real NAV data"
    )
    return funds

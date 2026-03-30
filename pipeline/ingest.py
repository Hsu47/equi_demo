"""
ingest.py — Simulates ingesting raw fund data from multiple opaque sources.

In the real world, Equi faces 12,600+ private funds, each reporting in different
formats: PDF tear sheets, Excel NAV tables, JSON APIs, or plain email text.
This module simulates that heterogeneity with 10 mock funds across CSV strings,
JSON strings, and raw Python dicts — exactly the normalization problem Equi solves.
"""

import json
import io
import csv
from typing import Union, List


# ---------------------------------------------------------------------------
# Raw "data dumps" simulating what Equi might receive from fund managers
# ---------------------------------------------------------------------------

# Fund A–C: JSON format (common from tech-forward managers)
FUND_A_JSON = json.dumps({
    "fund_id": "FUND_A",
    "name": "Apex Global Macro",
    "monthly_returns": [0.031, -0.012, 0.045, 0.008, -0.021, 0.033,
                        0.019, -0.007, 0.028, 0.041, -0.015, 0.022],
    "currency": "USD",
    "aum_mm": 420,
})

FUND_B_JSON = json.dumps({
    "fund_id": "FUND_B",
    "name": "BlueCrest Systematic",
    "monthly_returns": [0.018, 0.024, -0.009, 0.037, 0.011, -0.031,
                        0.042, 0.005, -0.018, 0.029, 0.013, 0.020],
    "currency": "USD",
    "aum_mm": 210,
})

FUND_C_JSON = json.dumps({
    "fund_id": "FUND_C",
    "name": "Citadel Quant Arb",
    "monthly_returns": [0.009, 0.015, 0.011, -0.004, 0.017, 0.012,
                        0.008, 0.021, -0.003, 0.014, 0.016, 0.010],
    "currency": "USD",
    "aum_mm": 880,
})

# Fund D–F: CSV strings (common from older/traditional managers)
FUND_D_CSV = """fund_id,name,month,return
FUND_D,Dalio All Weather,1,0.005
FUND_D,Dalio All Weather,2,0.008
FUND_D,Dalio All Weather,3,-0.003
FUND_D,Dalio All Weather,4,0.012
FUND_D,Dalio All Weather,5,0.006
FUND_D,Dalio All Weather,6,-0.001
FUND_D,Dalio All Weather,7,0.009
FUND_D,Dalio All Weather,8,0.004
FUND_D,Dalio All Weather,9,0.007
FUND_D,Dalio All Weather,10,-0.002
FUND_D,Dalio All Weather,11,0.011
FUND_D,Dalio All Weather,12,0.003"""

FUND_E_CSV = """fund_id,name,month,return
FUND_E,Elm Street Alpha,1,0.052
FUND_E,Elm Street Alpha,2,-0.031
FUND_E,Elm Street Alpha,3,0.078
FUND_E,Elm Street Alpha,4,-0.042
FUND_E,Elm Street Alpha,5,0.065
FUND_E,Elm Street Alpha,6,-0.028
FUND_E,Elm Street Alpha,7,0.091
FUND_E,Elm Street Alpha,8,-0.055
FUND_E,Elm Street Alpha,9,0.047
FUND_E,Elm Street Alpha,10,-0.019
FUND_E,Elm Street Alpha,11,0.083
FUND_E,Elm Street Alpha,12,-0.037"""

FUND_F_CSV = """fund_id,name,month,return
FUND_F,Fortress Credit Opp,1,0.022
FUND_F,Fortress Credit Opp,2,0.019
FUND_F,Fortress Credit Opp,3,0.025
FUND_F,Fortress Credit Opp,4,-0.008
FUND_F,Fortress Credit Opp,5,0.031
FUND_F,Fortress Credit Opp,6,0.017
FUND_F,Fortress Credit Opp,7,-0.004
FUND_F,Fortress Credit Opp,8,0.028
FUND_F,Fortress Credit Opp,9,0.023
FUND_F,Fortress Credit Opp,10,0.015
FUND_F,Fortress Credit Opp,11,-0.011
FUND_F,Fortress Credit Opp,12,0.020"""

# Fund G–J: Python dict format (from managers with basic API access)
FUND_G_DICT = {
    "fund_id": "FUND_G",
    "name": "GreenLight Value",
    "returns": [-0.041, 0.067, -0.028, 0.054, -0.019, 0.073,
                -0.033, 0.049, -0.015, 0.061, -0.022, 0.038],
    "aum_mm": 155,
}

FUND_H_DICT = {
    "fund_id": "FUND_H",
    "name": "Harbor Volatility",
    "returns": [0.003, -0.002, 0.005, 0.001, -0.003, 0.004,
                0.002, -0.001, 0.006, 0.003, -0.002, 0.004],
    "aum_mm": 75,
}

FUND_I_DICT = {
    "fund_id": "FUND_I",
    "name": "Invictus Merger Arb",
    "returns": [0.017, 0.021, 0.014, 0.019, 0.016, 0.022,
                0.018, 0.013, 0.020, 0.015, 0.023, 0.017],
    "aum_mm": 340,
}

FUND_J_DICT = {
    "fund_id": "FUND_J",
    "name": "Jupiter Distressed",
    "returns": [-0.062, 0.115, -0.048, 0.092, -0.071, 0.138,
                -0.053, 0.087, -0.044, 0.101, -0.058, 0.095],
    "aum_mm": 200,
}


# ---------------------------------------------------------------------------
# Ingest functions — one per format type
# ---------------------------------------------------------------------------

def ingest_json(raw: str) -> dict:
    """Parse a JSON string from a fund manager's API or data dump."""
    data = json.loads(raw)
    # JSON funds may use 'monthly_returns' key
    returns_key = "monthly_returns" if "monthly_returns" in data else "returns"
    return {
        "fund_id": data["fund_id"],
        "name": data["name"],
        "raw_returns": data[returns_key],
        "aum_mm": data.get("aum_mm", None),
        "source_format": "json",
    }


def ingest_csv(raw: str) -> dict:
    """Parse a CSV string where each row is one monthly return."""
    reader = csv.DictReader(io.StringIO(raw.strip()))
    rows = list(reader)
    returns = [float(r["return"]) for r in rows]
    return {
        "fund_id": rows[0]["fund_id"],
        "name": rows[0]["name"],
        "raw_returns": returns,
        "aum_mm": None,       # CSV format often omits AUM
        "source_format": "csv",
    }


def ingest_dict(raw: dict) -> dict:
    """Normalize a raw Python dict (e.g., from an internal API call)."""
    return {
        "fund_id": raw["fund_id"],
        "name": raw["name"],
        "raw_returns": raw["returns"],
        "aum_mm": raw.get("aum_mm", None),
        "source_format": "dict",
    }


# ---------------------------------------------------------------------------
# Main entry point: returns a list of normalized raw records
# ---------------------------------------------------------------------------

def load_all_funds() -> List[dict]:
    """
    Simulate ingesting data from 10 different fund managers.
    Returns a list of dicts with a unified schema:
        fund_id, name, raw_returns, aum_mm, source_format
    """
    funds = []

    # JSON sources
    for raw in [FUND_A_JSON, FUND_B_JSON, FUND_C_JSON]:
        funds.append(ingest_json(raw))

    # CSV sources
    for raw in [FUND_D_CSV, FUND_E_CSV, FUND_F_CSV]:
        funds.append(ingest_csv(raw))

    # Dict sources
    for raw in [FUND_G_DICT, FUND_H_DICT, FUND_I_DICT, FUND_J_DICT]:
        funds.append(ingest_dict(raw))

    print(f"[ingest] Loaded {len(funds)} funds from mixed formats "
          f"(json={3}, csv={3}, dict={4})")
    return funds

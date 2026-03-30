"""
app.py — Equi Fund Scoring Dashboard (Flask)

Serves a web dashboard that visualizes the full pipeline output:
  - Fund ranking table with RECOMMEND / WATCHLIST / PASS signals
  - Portfolio analytics: equal-weight RECOMMEND tier vs SPY
  - Cross-fund correlation matrix (the core Equi insight)
  - Undiscovered Gem highlights

Run: python app.py
Open: http://localhost:5001
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from flask import Flask, render_template, jsonify
from pipeline.ingest import load_all_funds
from pipeline.transform import transform_all
from pipeline.score import score_all, portfolio_analytics

app = Flask(__name__)

# ── Run pipeline once on startup ─────────────────────────────────────────────
raw      = load_all_funds()
tfmd     = transform_all(raw)
scored   = score_all(tfmd)
recommended = [f for f in scored if f["recommendation"] == "RECOMMEND"]
analytics   = portfolio_analytics(recommended)


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/funds")
def api_funds():
    """Ranked fund list for the main table."""
    out = []
    for rank, f in enumerate(scored, 1):
        out.append({
            "rank":         rank,
            "fund_id":      f["fund_id"],
            "name":         f["name"],
            "aum_mm":       f.get("aum_mm"),
            "ann_return":   f["annualized_return"],
            "sharpe":       f["sharpe_ratio"],
            "max_dd":       f["max_drawdown"],
            "sortino":      f["sortino_ratio"],
            "corr":         f["market_correlation"],
            "score":        f["composite_score"],
            "rec":          f["recommendation"],
            "gem":          f.get("gem", False),
        })
    return jsonify(out)


@app.route("/api/portfolio")
def api_portfolio():
    """Portfolio-level analytics for the comparison section."""
    if not analytics:
        return jsonify({})

    # Build ordered fund names for the correlation matrix
    funds = analytics["funds_in_portfolio"]
    n = len(funds)
    matrix = []
    for i in range(n):
        row = []
        for j in range(n):
            if i == j:
                row.append(1.0)
            else:
                k1 = f"{funds[i][:12]} × {funds[j][:12]}"
                k2 = f"{funds[j][:12]} × {funds[i][:12]}"
                val = analytics["correlation_matrix"].get(k1) or \
                      analytics["correlation_matrix"].get(k2, 0.0)
                row.append(val)
        matrix.append(row)

    return jsonify({
        "funds":              funds,
        "port_return":        analytics["portfolio_annual_return"],
        "spy_return":         analytics["spy_annual_return"],
        "port_sharpe":        analytics["portfolio_sharpe"],
        "spy_sharpe":         analytics["spy_sharpe"],
        "port_dd":            analytics["portfolio_max_dd"],
        "spy_dd":             analytics["spy_max_dd"],
        "correlation_matrix": matrix,
    })


if __name__ == "__main__":
    print("\n  Equi Fund Scoring Dashboard → http://localhost:5001\n")
    app.run(debug=False, port=5001)

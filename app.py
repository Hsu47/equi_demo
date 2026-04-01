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
import time
import datetime
sys.path.insert(0, os.path.dirname(__file__))

from flask import Flask, render_template, jsonify, send_from_directory
from pipeline.ingest import load_all_funds, load_private_funds
from pipeline.ingest_live import load_live_funds
from pipeline.transform import transform_all
from pipeline.score import score_all, portfolio_analytics

app = Flask(__name__)

# ── Run pipeline once on startup (with timing) ────────────────────────────────
_t0 = time.time()

_t_ingest_start = time.time()
try:
    live = load_live_funds()
    _data_source = "live"
    if len(live) < 5:          # too few live results → fall back
        raise ValueError("insufficient live data")
except Exception as _e:
    print(f"[app] Live data failed ({_e}), falling back to mock data")
    live = load_all_funds()
    _data_source = "mock"
# Always blend in synthetic private funds (Equi's off-market GP universe)
raw = live + load_private_funds()
_t_ingest_ms = round((time.time() - _t_ingest_start) * 1000, 1)

_t_transform_start = time.time()
tfmd     = transform_all(raw)
_t_transform_ms = round((time.time() - _t_transform_start) * 1000, 1)

_t_score_start = time.time()
scored   = score_all(tfmd)
_t_score_ms = round((time.time() - _t_score_start) * 1000, 1)

recommended = [f for f in scored if f["recommendation"] == "RECOMMEND"]
analytics   = portfolio_analytics(recommended)

_pipeline_total_ms = round((time.time() - _t0) * 1000, 1)
_pipeline_ran_at   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

_format_counts = {"json": 0, "csv": 0, "dict": 0, "live": 0}
for f in raw:
    key = f.get("source_format", "dict")
    _format_counts[key] = _format_counts.get(key, 0) + 1


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
            "rank":                   rank,
            "fund_id":                f["fund_id"],
            "name":                   f["name"],
            "aum_mm":                 f.get("aum_mm"),
            "ann_return":             f["annualized_return"],
            "sharpe":                 f["sharpe_ratio"],
            "max_dd":                 f["max_drawdown"],
            "sortino":                f["sortino_ratio"],
            "corr":                   f["market_correlation"],
            "score":                  f["composite_score"],
            "rec":                    f["recommendation"],
            "gem":                    f.get("gem", False),
            "gem_tier":               "strong" if (f.get("composite_score",0) >= 85 and (f.get("aum_mm") or 999) < 500)
                                      else ("emerging" if f.get("gem") else None),
            # Regime-conditional correlation fields
            "calm_corr":              f.get("calm_correlation"),
            "stress_corr":            f.get("stress_correlation"),
            "regime_shift":           f.get("correlation_regime_shift"),
            "regime_risk_flag":       f.get("regime_risk_flag", False),
            "regime_data_limited":    f.get("regime_data_limited", False),
        })
    return jsonify(out)


@app.route("/api/regime")
def api_regime():
    """
    Regime-conditional correlation analysis for all funds.
    Returns per-fund calm/stress correlation data, VIX regime summary,
    and a list of funds flagged for regime risk.
    """
    from pipeline.transform import get_vix_regime_labels
    vix_labels = get_vix_regime_labels()

    calm_count   = vix_labels.count("calm")
    stress_count = vix_labels.count("stress")

    fund_data = []
    flagged   = []
    for f in scored:
        entry = {
            "fund_id":             f["fund_id"],
            "name":                f["name"],
            "rec":                 f["recommendation"],
            "full_correlation":    f["market_correlation"],
            "calm_correlation":    f.get("calm_correlation"),
            "stress_correlation":  f.get("stress_correlation"),
            "regime_shift":        f.get("correlation_regime_shift"),
            "regime_risk_flag":    f.get("regime_risk_flag", False),
            "regime_data_limited": f.get("regime_data_limited", False),
            "calm_months":         f.get("calm_months", 0),
            "stress_months":       f.get("stress_months", 0),
        }
        fund_data.append(entry)
        if f.get("regime_risk_flag"):
            flagged.append(f["name"])

    return jsonify({
        "vix_regime_summary": {
            "total_months":  len(vix_labels),
            "calm_months":   calm_count,
            "stress_months": stress_count,
            "labels":        vix_labels,
        },
        "funds":             fund_data,
        "flagged_funds":     flagged,
    })


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


@app.route("/api/fee_arbitrage")
def api_fee_arbitrage():
    """
    Equi's first-layer alpha: fee negotiation on undiscovered managers.

    Tory has stated publicly that Equi negotiates manager fees down 50-60%
    on average. This endpoint simulates the compounding impact of that fee
    discount over a 5-year horizon vs paying full market-rate fees.

    Standard hedge fund fee: 2% management + 20% performance (2/20)
    Equi-negotiated fee:     1% management + 10% performance (1/10)

    Gross return assumption: 15% annually (realistic for top-quartile alt managers)
    """
    gross_annual = 0.15
    years        = 5
    initial      = 1_000_000  # $1M baseline

    standard_mgmt = 0.02
    standard_perf = 0.20
    equi_mgmt     = 0.01
    equi_perf     = 0.10

    standard_curve, equi_curve = [initial], [initial]

    for _ in range(years):
        # Standard fees
        prev = standard_curve[-1]
        gross = prev * (1 + gross_annual)
        profit = gross - prev
        net = gross - prev * standard_mgmt - profit * standard_perf
        standard_curve.append(round(net, 2))

        # Equi-negotiated fees
        prev = equi_curve[-1]
        gross = prev * (1 + gross_annual)
        profit = gross - prev
        net = gross - prev * equi_mgmt - profit * equi_perf
        equi_curve.append(round(net, 2))

    fee_savings_5yr = round(equi_curve[-1] - standard_curve[-1], 0)
    pct_uplift      = round((equi_curve[-1] / standard_curve[-1] - 1) * 100, 1)

    return jsonify({
        "labels":          list(range(years + 1)),
        "standard_curve":  standard_curve,
        "equi_curve":      equi_curve,
        "fee_savings_5yr": fee_savings_5yr,
        "pct_uplift":      pct_uplift,
        "initial":         initial,
        "gross_annual_pct": gross_annual * 100,
    })


@app.route("/api/meta")
def api_meta():
    """Pipeline execution metadata — proves the system actually ran."""
    rec   = len([f for f in scored if f["recommendation"] == "RECOMMEND"])
    watch = len([f for f in scored if f["recommendation"] == "WATCHLIST"])
    fail  = len([f for f in scored if f["recommendation"] == "PASS"])
    gems  = len([f for f in scored if f.get("gem")])
    return jsonify({
        "ran_at":          _pipeline_ran_at,
        "total_funds":     len(raw),
        "data_source":     _data_source,
        "formats":         _format_counts,
        "transform_pass":  len(tfmd),
        "recommend":       rec,
        "watchlist":       watch,
        "pass_count":      fail,
        "gems":            gems,
        "timing": {
            "ingest_ms":    _t_ingest_ms,
            "transform_ms": _t_transform_ms,
            "score_ms":     _t_score_ms,
            "total_ms":     _pipeline_total_ms,
        }
    })


@app.route("/api/moat")
def api_moat():
    """Competitive differentiation vs iCapital, CAIS, PitchBook."""
    return jsonify([
        {
            "competitor": "iCapital",
            "model":      "Distribution platform — aggregates other managers' products",
            "equi_diff":  "Equi selects & manages directly. Alpha claim is Equi's own.",
            "icon": "📦",
        },
        {
            "competitor": "CAIS",
            "model":      "62K advisors, fee cut to 0.05% — competing on volume",
            "equi_diff":  "Equi competes on quality + uncorrelated return, not price.",
            "icon": "📊",
        },
        {
            "competitor": "PitchBook / AlternativeSoft",
            "model":      "Sell analytics tools to institutional buyers",
            "equi_diff":  "Equi's scoring feeds its own allocation. Tool = process.",
            "icon": "🛠️",
        },
        {
            "competitor": "Moonfare / Titanbay",
            "model":      "PE/private markets access for HNW, secondary liquidity",
            "equi_diff":  "Equi targets hedge fund strategies, liquid alternatives, low corr.",
            "icon": "🌙",
        },
    ])


@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory(
        os.path.join(os.path.dirname(__file__), "static"), filename
    )


@app.route("/api/pdf_demo")
def api_pdf_demo():
    """Live PDF extraction demo: parse sample LP report → transform → score."""
    import time as _time
    from pipeline.ingest_pdf import load_fund_from_pdf
    from pipeline.transform import transform_fund
    from pipeline.score import (composite_score, sharpe_ratio, max_drawdown,
                                 sortino_ratio, pearson_correlation)
    from pipeline.transform import get_spy_monthly

    pdf_path = os.path.join(os.path.dirname(__file__), "static", "sample_lp_report.pdf")

    t0 = _time.time()
    try:
        fund_raw  = load_fund_from_pdf(pdf_path)
        t_extract = round((_time.time() - t0) * 1000, 1)

        transformed = transform_fund(fund_raw)
        excess  = transformed["excess_returns"]      # rf already subtracted — for Sharpe
        monthly = transformed["monthly_returns"]     # raw monthly — for Sortino, Corr
        spy     = transformed.get("market_returns") or get_spy_monthly()[:len(monthly)]

        from pipeline.score import recommend as score_recommend
        sharpe  = sharpe_ratio(excess)
        dd      = max_drawdown(transformed["nav_curve"])
        sortino = sortino_ratio(monthly)             # sortino_ratio subtracts rf internally
        corr    = pearson_correlation(monthly, spy[:len(monthly)])
        # composite_score(sharpe, drawdown, sortino, correlation, aum_mm)
        score  = composite_score(sharpe, dd, sortino, corr,
                                 aum_mm=fund_raw.get("aum_mm"))
        signal = score_recommend(sharpe, dd, corr, score)

        return jsonify({
            "status":        "success",
            "fund_name":     fund_raw["name"],
            "aum_mm":        fund_raw["aum_mm"],
            "currency":      fund_raw.get("currency", "USD"),
            "mgmt_fee_pct":  fund_raw.get("mgmt_fee_pct"),
            "incentive_fee_pct": fund_raw.get("incentive_fee_pct"),
            "source_format": "pdf",
            "extraction":    fund_raw["extraction"],
            "extract_ms":    t_extract,
            "raw_returns_preview": [round(r * 100, 2) for r in fund_raw["raw_returns"]],
            "metrics": {
                "sharpe":   round(sharpe,  3),
                "max_dd":   round(dd,      3),
                "sortino":  round(sortino, 3),
                "corr_spy": round(corr,    3),
                "score":    score,
                "signal":   signal,
            },
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    print("\n  Equi Fund Scoring Dashboard → http://localhost:5001\n")
    app.run(debug=False, port=5001)

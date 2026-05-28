from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


EQUITY_COMPONENTS = [
    {
        "component": "valuation_compression_pressure",
        "raw_score": 0.41,
        "weight": 0.22,
        "recursive_channel": "valuation_reflexivity_loop",
        "iv_vector": "V",
    },
    {
        "component": "earnings_revision_pressure",
        "raw_score": 0.39,
        "weight": 0.20,
        "recursive_channel": "earnings_fragility_loop",
        "iv_vector": "E",
    },
    {
        "component": "liquidity_beta_pressure",
        "raw_score": 0.38,
        "weight": 0.18,
        "recursive_channel": "liquidity_beta_loop",
        "iv_vector": "L",
    },
    {
        "component": "credit_to_equity_pressure",
        "raw_score": 0.40,
        "weight": 0.16,
        "recursive_channel": "credit_equity_transmission_loop",
        "iv_vector": "C",
    },
    {
        "component": "positioning_reflexivity_pressure",
        "raw_score": 0.43,
        "weight": 0.14,
        "recursive_channel": "positioning_reflexivity_loop",
        "iv_vector": "P",
    },
    {
        "component": "volatility_feedback_pressure",
        "raw_score": 0.37,
        "weight": 0.10,
        "recursive_channel": "volatility_feedback_loop",
        "iv_vector": "X",
    },
]


def load_json(path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def classify_equity_state(score):
    if score >= 0.75:
        return "systemic_equity_recursion"

    if score >= 0.60:
        return "fragile_equity_recursion"

    if score >= 0.40:
        return "elevated_equity_recursion"

    if score >= 0.25:
        return "watch_equity_recursion"

    return "stable_equity_recursion"


def build_equities_recursive_propagation_engine_v1():
    repo_root = Path.cwd()

    out_dir = (
        repo_root
        / "data"
        / "equities"
        / "recursive"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    credit_summary = load_json(
        repo_root
        / "data"
        / "credit"
        / "recursive"
        / "credit_recursive_topology_engine_summary_v1.json"
    )

    macro_summary = load_json(
        repo_root
        / "data"
        / "macroecon"
        / "recursive"
        / "macroecon_recursive_engine_summary_v1.json"
    )

    cb_summary = load_json(
        repo_root
        / "data"
        / "cb"
        / "recursive"
        / "central_bank_recursive_engine_summary_v1.json"
    )

    cot_summary = load_json(
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_iv_vector_summary_v1.json"
    )

    credit_pressure = float(
        credit_summary.get(
            "macro_adjusted_credit_pressure",
            0.0,
        )
        or 0.0
    )

    macro_pressure = float(
        macro_summary.get(
            "cb_adjusted_macro_pressure",
            0.0,
        )
        or 0.0
    )

    cb_pressure = float(
        cb_summary.get(
            "central_bank_recursive_pressure",
            0.0,
        )
        or 0.0
    )

    cot_transition_pressure = float(
        cot_summary.get(
            "cot_iv_transition_pressure",
            0.0,
        )
        or 0.0
    )

    credit_regime = credit_summary.get(
        "dominant_credit_regime",
        "unknown",
    )

    macro_regime = macro_summary.get(
        "macro_regime_bias",
        "unknown",
    )

    cb_bias = cb_summary.get(
        "dominant_cb_bias",
        "unknown",
    )

    df = pd.DataFrame(EQUITY_COMPONENTS)

    df["weighted_score"] = (
        df["raw_score"]
        * df["weight"]
    ).round(4)

    equity_recursive_pressure = round(
        float(df["weighted_score"].sum()),
        4,
    )

    upstream_adjusted_equity_pressure = round(
        min(
            1.0,
            0.58 * equity_recursive_pressure
            + 0.18 * credit_pressure
            + 0.10 * macro_pressure
            + 0.08 * cb_pressure
            + 0.06 * cot_transition_pressure,
        ),
        4,
    )

    max_component_score = round(
        float(df["raw_score"].max()),
        4,
    )

    equity_recursive_state = classify_equity_state(
        upstream_adjusted_equity_pressure
    )

    valuation_pressure = float(
        df.loc[
            df["component"] == "valuation_compression_pressure",
            "raw_score",
        ].iloc[0]
    )

    earnings_pressure = float(
        df.loc[
            df["component"] == "earnings_revision_pressure",
            "raw_score",
        ].iloc[0]
    )

    liquidity_beta_pressure = float(
        df.loc[
            df["component"] == "liquidity_beta_pressure",
            "raw_score",
        ].iloc[0]
    )

    positioning_pressure = float(
        df.loc[
            df["component"] == "positioning_reflexivity_pressure",
            "raw_score",
        ].iloc[0]
    )

    volatility_pressure = float(
        df.loc[
            df["component"] == "volatility_feedback_pressure",
            "raw_score",
        ].iloc[0]
    )

    equity_reflexivity_score = round(
        0.28 * valuation_pressure
        + 0.24 * earnings_pressure
        + 0.20 * positioning_pressure
        + 0.16 * liquidity_beta_pressure
        + 0.12 * volatility_pressure,
        4,
    )

    if (
        credit_regime == "refinancing_fragility_regime"
        and earnings_pressure >= 0.38
    ):
        dominant_equity_regime = "credit_earnings_reflexivity_regime"

    elif (
        macro_regime == "inflation_constrained_slowdown"
        and valuation_pressure >= 0.40
    ):
        dominant_equity_regime = "valuation_compression_regime"

    elif positioning_pressure >= 0.45:
        dominant_equity_regime = "positioning_reflexivity_regime"

    else:
        dominant_equity_regime = "watch_equity_regime"

    propagation_links = [
        {
            "source": "credit",
            "target": "equities",
            "transmission_channel": "earnings_reflexivity",
            "recursive_feedback": round(credit_pressure * 0.12, 4),
        },
        {
            "source": "macroecon",
            "target": "equities",
            "transmission_channel": "growth_valuation_pressure",
            "recursive_feedback": round(macro_pressure * 0.10, 4),
        },
        {
            "source": "central_bank",
            "target": "equities",
            "transmission_channel": "discount_rate_constraint",
            "recursive_feedback": round(cb_pressure * 0.10, 4),
        },
        {
            "source": "cot",
            "target": "equities",
            "transmission_channel": "positioning_transition_pressure",
            "recursive_feedback": round(cot_transition_pressure * 0.11, 4),
        },
        {
            "source": "equities",
            "target": "finstate",
            "transmission_channel": "wealth_balance_sheet_reflexivity",
            "recursive_feedback": round(equity_recursive_pressure * 0.12, 4),
        },
        {
            "source": "equities",
            "target": "credit",
            "transmission_channel": "equity_credit_confidence_loop",
            "recursive_feedback": round(equity_recursive_pressure * 0.10, 4),
        },
    ]

    equity_paths = [
        {
            "equity_path": "contained_equity_watch",
            "probability_proxy": round(
                max(
                    0.0,
                    1.0 - upstream_adjusted_equity_pressure,
                ),
                4,
            ),
            "interpretation": "Equity stress remains contained within watch-level recursive conditions.",
        },
        {
            "equity_path": "valuation_compression",
            "probability_proxy": round(
                min(
                    1.0,
                    0.45 * valuation_pressure
                    + 0.30 * cb_pressure
                    + 0.25 * macro_pressure,
                ),
                4,
            ),
            "interpretation": "Higher discount-rate pressure compresses equity valuations.",
        },
        {
            "equity_path": "credit_earnings_reflexivity",
            "probability_proxy": round(
                min(
                    1.0,
                    0.40 * credit_pressure
                    + 0.35 * earnings_pressure
                    + 0.25 * macro_pressure,
                ),
                4,
            ),
            "interpretation": "Credit fragility begins feeding earnings downside risk.",
        },
        {
            "equity_path": "positioning_volatility_feedback",
            "probability_proxy": round(
                min(
                    1.0,
                    0.45 * positioning_pressure
                    + 0.30 * volatility_pressure
                    + 0.25 * cot_transition_pressure,
                ),
                4,
            ),
            "interpretation": "Crowded positioning and volatility feedback reinforce drawdown risk.",
        },
    ]

    parquet_path = (
        out_dir
        / "equities_recursive_propagation_engine_v1.parquet"
    )

    json_path = (
        out_dir
        / "equities_recursive_propagation_engine_v1.json"
    )

    links_path = (
        out_dir
        / "equities_recursive_propagation_links_v1.json"
    )

    paths_path = (
        out_dir
        / "equities_recursive_paths_v1.json"
    )

    summary_path = (
        out_dir
        / "equities_recursive_propagation_engine_summary_v1.json"
    )

    df.to_parquet(
        parquet_path,
        index=False,
    )

    df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(links_path, "w", encoding="utf-8") as f:
        json.dump(
            propagation_links,
            f,
            indent=2,
        )

    with open(paths_path, "w", encoding="utf-8") as f:
        json.dump(
            equity_paths,
            f,
            indent=2,
        )

    summary = {
        "component": "equities_recursive_propagation_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "equity_recursive_pressure": equity_recursive_pressure,
        "upstream_adjusted_equity_pressure": upstream_adjusted_equity_pressure,
        "equity_recursive_state": equity_recursive_state,
        "dominant_equity_regime": dominant_equity_regime,
        "equity_reflexivity_score": equity_reflexivity_score,
        "credit_regime": credit_regime,
        "macro_regime": macro_regime,
        "cb_bias": cb_bias,
        "credit_pressure": round(credit_pressure, 4),
        "macro_pressure": round(macro_pressure, 4),
        "cb_pressure": round(cb_pressure, 4),
        "cot_transition_pressure": round(cot_transition_pressure, 4),
        "max_component_score": max_component_score,
        "iv_vector_targets": sorted(
            df["iv_vector"].unique().tolist()
        ),
        "active_components": df["component"].tolist(),
        "propagation_links": propagation_links,
        "equity_paths": equity_paths,
        "status": "equities_recursive_propagation_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            indent=2,
        )

    print("Equities recursive propagation engine complete")
    print("Rows:", len(df))
    print("Equity Recursive Pressure:", equity_recursive_pressure)
    print("Upstream Adjusted Equity Pressure:", upstream_adjusted_equity_pressure)
    print("Equity Recursive State:", equity_recursive_state)
    print("Dominant Equity Regime:", dominant_equity_regime)
    print("Equity Reflexivity Score:", equity_reflexivity_score)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("LINKS:", links_path)
    print("PATHS:", paths_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_equities_recursive_propagation_engine_v1()

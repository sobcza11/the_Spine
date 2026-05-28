from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


COMMODITY_COMPONENTS = [
    {
        "component": "energy_inflation_pressure",
        "raw_score": 0.42,
        "weight": 0.22,
        "recursive_channel": "energy_inflation_loop",
        "iv_vector": "E",
    },
    {
        "component": "industrial_metals_growth_pressure",
        "raw_score": 0.37,
        "weight": 0.18,
        "recursive_channel": "growth_demand_loop",
        "iv_vector": "G",
    },
    {
        "component": "precious_metals_real_rate_pressure",
        "raw_score": 0.40,
        "weight": 0.17,
        "recursive_channel": "real_rate_reflexivity_loop",
        "iv_vector": "R",
    },
    {
        "component": "commodity_fx_pass_through_pressure",
        "raw_score": 0.39,
        "weight": 0.16,
        "recursive_channel": "fx_inflation_pass_through_loop",
        "iv_vector": "X",
    },
    {
        "component": "agriculture_supply_pressure",
        "raw_score": 0.34,
        "weight": 0.13,
        "recursive_channel": "food_inflation_loop",
        "iv_vector": "F",
    },
    {
        "component": "commodity_liquidity_pressure",
        "raw_score": 0.36,
        "weight": 0.14,
        "recursive_channel": "commodity_liquidity_loop",
        "iv_vector": "L",
    },
]


def load_json(path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def classify_commodity_state(score):
    if score >= 0.75:
        return "systemic_commodity_recursion"

    if score >= 0.60:
        return "fragile_commodity_recursion"

    if score >= 0.40:
        return "elevated_commodity_recursion"

    if score >= 0.25:
        return "watch_commodity_recursion"

    return "stable_commodity_recursion"


def build_commodities_recursive_reflexivity_engine_v1():
    repo_root = Path.cwd()

    out_dir = (
        repo_root
        / "data"
        / "commodities"
        / "recursive"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
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

    fx_summary = load_json(
        repo_root
        / "data"
        / "fx"
        / "recursive"
        / "fx_recursive_stress_summary_v1.json"
    )

    rates_summary = load_json(
        repo_root
        / "data"
        / "rates"
        / "recursive"
        / "rates_recursive_pressure_summary_v1.json"
    )

    cot_summary = load_json(
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_iv_vector_summary_v1.json"
    )

    macro_pressure = float(
        macro_summary.get("cb_adjusted_macro_pressure", 0.0) or 0.0
    )

    cb_pressure = float(
        cb_summary.get("central_bank_recursive_pressure", 0.0) or 0.0
    )

    fx_pressure = float(
        fx_summary.get("fx_recursive_pressure", 0.0) or 0.0
    )

    rates_pressure = float(
        rates_summary.get("rates_recursive_pressure", 0.0) or 0.0
    )

    cot_transition_pressure = float(
        cot_summary.get("cot_iv_transition_pressure", 0.0) or 0.0
    )

    macro_regime = macro_summary.get("macro_regime_bias", "unknown")
    cb_bias = cb_summary.get("dominant_cb_bias", "unknown")
    fx_state = fx_summary.get("fx_recursive_state", "unknown")

    df = pd.DataFrame(COMMODITY_COMPONENTS)

    df["weighted_score"] = (
        df["raw_score"]
        * df["weight"]
    ).round(4)

    commodity_recursive_pressure = round(
        float(df["weighted_score"].sum()),
        4,
    )

    upstream_adjusted_commodity_pressure = round(
        min(
            1.0,
            0.58 * commodity_recursive_pressure
            + 0.14 * macro_pressure
            + 0.11 * cb_pressure
            + 0.09 * fx_pressure
            + 0.05 * rates_pressure
            + 0.03 * cot_transition_pressure,
        ),
        4,
    )

    max_component_score = round(
        float(df["raw_score"].max()),
        4,
    )

    commodity_recursive_state = classify_commodity_state(
        upstream_adjusted_commodity_pressure
    )

    energy_pressure = float(
        df.loc[
            df["component"] == "energy_inflation_pressure",
            "raw_score",
        ].iloc[0]
    )

    metals_pressure = float(
        df.loc[
            df["component"] == "industrial_metals_growth_pressure",
            "raw_score",
        ].iloc[0]
    )

    precious_metals_pressure = float(
        df.loc[
            df["component"] == "precious_metals_real_rate_pressure",
            "raw_score",
        ].iloc[0]
    )

    fx_pass_through_pressure = float(
        df.loc[
            df["component"] == "commodity_fx_pass_through_pressure",
            "raw_score",
        ].iloc[0]
    )

    agriculture_pressure = float(
        df.loc[
            df["component"] == "agriculture_supply_pressure",
            "raw_score",
        ].iloc[0]
    )

    commodity_liquidity_pressure = float(
        df.loc[
            df["component"] == "commodity_liquidity_pressure",
            "raw_score",
        ].iloc[0]
    )

    commodity_reflexivity_score = round(
        0.24 * energy_pressure
        + 0.18 * fx_pass_through_pressure
        + 0.17 * precious_metals_pressure
        + 0.16 * metals_pressure
        + 0.13 * commodity_liquidity_pressure
        + 0.12 * agriculture_pressure,
        4,
    )

    if (
        macro_regime == "inflation_constrained_slowdown"
        and energy_pressure >= 0.40
        and cb_bias == "hawkish_constraint"
    ):
        dominant_commodity_regime = "inflation_reflexivity_regime"

    elif (
        fx_state == "elevated_fx_stress"
        and fx_pass_through_pressure >= 0.38
    ):
        dominant_commodity_regime = "fx_commodity_pass_through_regime"

    elif metals_pressure >= 0.42:
        dominant_commodity_regime = "growth_sensitive_commodity_regime"

    else:
        dominant_commodity_regime = "watch_commodity_regime"

    propagation_links = [
        {
            "source": "commodities",
            "target": "macroecon",
            "transmission_channel": "inflation_feedback",
            "recursive_feedback": round(commodity_recursive_pressure * 0.12, 4),
        },
        {
            "source": "commodities",
            "target": "central_bank",
            "transmission_channel": "policy_constraint_feedback",
            "recursive_feedback": round(commodity_recursive_pressure * 0.11, 4),
        },
        {
            "source": "fx",
            "target": "commodities",
            "transmission_channel": "usd_pass_through",
            "recursive_feedback": round(fx_pressure * 0.10, 4),
        },
        {
            "source": "rates",
            "target": "commodities",
            "transmission_channel": "real_rate_channel",
            "recursive_feedback": round(rates_pressure * 0.09, 4),
        },
        {
            "source": "commodities",
            "target": "finstate",
            "transmission_channel": "margin_inflation_pressure",
            "recursive_feedback": round(commodity_recursive_pressure * 0.10, 4),
        },
        {
            "source": "commodities",
            "target": "equities",
            "transmission_channel": "input_cost_margin_pressure",
            "recursive_feedback": round(commodity_recursive_pressure * 0.09, 4),
        },
    ]

    commodity_paths = [
        {
            "commodity_path": "contained_commodity_watch",
            "probability_proxy": round(
                max(
                    0.0,
                    1.0 - upstream_adjusted_commodity_pressure,
                ),
                4,
            ),
            "interpretation": "Commodity reflexivity remains active but contained.",
        },
        {
            "commodity_path": "inflation_reflexivity",
            "probability_proxy": round(
                min(
                    1.0,
                    0.45 * energy_pressure
                    + 0.30 * cb_pressure
                    + 0.25 * macro_pressure,
                ),
                4,
            ),
            "interpretation": "Commodity inflation reinforces policy constraint pressure.",
        },
        {
            "commodity_path": "fx_pass_through_reflexivity",
            "probability_proxy": round(
                min(
                    1.0,
                    0.45 * fx_pass_through_pressure
                    + 0.35 * fx_pressure
                    + 0.20 * commodity_liquidity_pressure,
                ),
                4,
            ),
            "interpretation": "FX stress passes through commodities into inflation channels.",
        },
        {
            "commodity_path": "growth_sensitive_demand_softening",
            "probability_proxy": round(
                min(
                    1.0,
                    0.40 * metals_pressure
                    + 0.35 * macro_pressure
                    + 0.25 * rates_pressure,
                ),
                4,
            ),
            "interpretation": "Industrial commodity pressure reflects growth sensitivity.",
        },
    ]

    parquet_path = (
        out_dir
        / "commodities_recursive_reflexivity_engine_v1.parquet"
    )

    json_path = (
        out_dir
        / "commodities_recursive_reflexivity_engine_v1.json"
    )

    links_path = (
        out_dir
        / "commodities_recursive_propagation_links_v1.json"
    )

    paths_path = (
        out_dir
        / "commodities_recursive_paths_v1.json"
    )

    summary_path = (
        out_dir
        / "commodities_recursive_reflexivity_engine_summary_v1.json"
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
            commodity_paths,
            f,
            indent=2,
        )

    summary = {
        "component": "commodities_recursive_reflexivity_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "commodity_recursive_pressure": commodity_recursive_pressure,
        "upstream_adjusted_commodity_pressure": upstream_adjusted_commodity_pressure,
        "commodity_recursive_state": commodity_recursive_state,
        "dominant_commodity_regime": dominant_commodity_regime,
        "commodity_reflexivity_score": commodity_reflexivity_score,
        "macro_regime": macro_regime,
        "cb_bias": cb_bias,
        "fx_state": fx_state,
        "macro_pressure": round(macro_pressure, 4),
        "cb_pressure": round(cb_pressure, 4),
        "fx_pressure": round(fx_pressure, 4),
        "rates_pressure": round(rates_pressure, 4),
        "cot_transition_pressure": round(cot_transition_pressure, 4),
        "max_component_score": max_component_score,
        "iv_vector_targets": sorted(
            df["iv_vector"].unique().tolist()
        ),
        "active_components": df["component"].tolist(),
        "propagation_links": propagation_links,
        "commodity_paths": commodity_paths,
        "status": "commodities_recursive_reflexivity_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            indent=2,
        )

    print("Commodities recursive reflexivity engine complete")
    print("Rows:", len(df))
    print("Commodity Recursive Pressure:", commodity_recursive_pressure)
    print("Upstream Adjusted Commodity Pressure:", upstream_adjusted_commodity_pressure)
    print("Commodity Recursive State:", commodity_recursive_state)
    print("Dominant Commodity Regime:", dominant_commodity_regime)
    print("Commodity Reflexivity Score:", commodity_reflexivity_score)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("LINKS:", links_path)
    print("PATHS:", paths_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_commodities_recursive_reflexivity_engine_v1()

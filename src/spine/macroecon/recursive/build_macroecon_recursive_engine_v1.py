from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


MACROECON_COMPONENTS = [
    {
        "component": "growth_fragility_pressure",
        "raw_score": 0.39,
        "weight": 0.22,
        "recursive_channel": "growth_slowdown_loop",
        "iv_vector": "G",
    },
    {
        "component": "inflation_persistence_pressure",
        "raw_score": 0.42,
        "weight": 0.22,
        "recursive_channel": "inflation_constraint_loop",
        "iv_vector": "F",
    },
    {
        "component": "liquidity_cycle_pressure",
        "raw_score": 0.38,
        "weight": 0.18,
        "recursive_channel": "liquidity_cycle_loop",
        "iv_vector": "L",
    },
    {
        "component": "labor_market_softening_pressure",
        "raw_score": 0.33,
        "weight": 0.14,
        "recursive_channel": "labor_fragility_loop",
        "iv_vector": "E",
    },
    {
        "component": "demand_deterioration_pressure",
        "raw_score": 0.36,
        "weight": 0.14,
        "recursive_channel": "demand_fragility_loop",
        "iv_vector": "D",
    },
    {
        "component": "macro_uncertainty_pressure",
        "raw_score": 0.41,
        "weight": 0.10,
        "recursive_channel": "uncertainty_feedback_loop",
        "iv_vector": "S",
    },
]


def classify_macroecon_state(score):
    if score >= 0.75:
        return "systemic_macroecon_recursion"

    if score >= 0.60:
        return "fragile_macroecon_recursion"

    if score >= 0.40:
        return "elevated_macroecon_recursion"

    if score >= 0.25:
        return "watch_macroecon_recursion"

    return "stable_macroecon_recursion"


def build_macroecon_recursive_engine_v1():
    repo_root = Path.cwd()

    out_dir = repo_root / "data" / "macroecon" / "recursive"
    out_dir.mkdir(parents=True, exist_ok=True)

    cb_summary_path = (
        repo_root
        / "data"
        / "cb"
        / "recursive"
        / "central_bank_recursive_engine_summary_v1.json"
    )

    if cb_summary_path.exists():
        with open(cb_summary_path, "r", encoding="utf-8") as f:
            cb_summary = json.load(f)
    else:
        cb_summary = {}

    cb_pressure = float(
        cb_summary.get("central_bank_recursive_pressure", 0.0) or 0.0
    )

    cb_bias = cb_summary.get("dominant_cb_bias", "unknown")

    df = pd.DataFrame(MACROECON_COMPONENTS)

    df["weighted_score"] = (
        df["raw_score"]
        * df["weight"]
    ).round(4)

    macroecon_recursive_pressure = round(
        float(df["weighted_score"].sum()),
        4,
    )

    cb_adjusted_macro_pressure = round(
        min(
            1.0,
            0.82 * macroecon_recursive_pressure
            + 0.18 * cb_pressure,
        ),
        4,
    )

    max_component_score = round(
        float(df["raw_score"].max()),
        4,
    )

    macroecon_recursive_state = classify_macroecon_state(
        cb_adjusted_macro_pressure
    )

    growth_pressure = float(
        df.loc[
            df["component"] == "growth_fragility_pressure",
            "raw_score",
        ].iloc[0]
    )

    inflation_pressure = float(
        df.loc[
            df["component"] == "inflation_persistence_pressure",
            "raw_score",
        ].iloc[0]
    )

    liquidity_pressure = float(
        df.loc[
            df["component"] == "liquidity_cycle_pressure",
            "raw_score",
        ].iloc[0]
    )

    demand_pressure = float(
        df.loc[
            df["component"] == "demand_deterioration_pressure",
            "raw_score",
        ].iloc[0]
    )

    macro_policy_tension = round(
        abs(inflation_pressure - growth_pressure),
        4,
    )

    if inflation_pressure > growth_pressure and cb_bias == "hawkish_constraint":
        macro_regime_bias = "inflation_constrained_slowdown"

    elif growth_pressure > inflation_pressure:
        macro_regime_bias = "growth_support_pressure"

    elif liquidity_pressure >= 0.40:
        macro_regime_bias = "liquidity_sensitive_macro_regime"

    else:
        macro_regime_bias = "balanced_macro_watch"

    macro_paths = [
        {
            "macro_path": "soft_landing_watch",
            "probability_proxy": round(
                max(
                    0.0,
                    1.0 - cb_adjusted_macro_pressure,
                ),
                4,
            ),
            "interpretation": "Macro stress remains contained under policy credibility.",
        },
        {
            "macro_path": "inflation_constrained_slowdown",
            "probability_proxy": round(
                min(
                    1.0,
                    0.45 * inflation_pressure
                    + 0.35 * cb_pressure
                    + 0.20 * growth_pressure,
                ),
                4,
            ),
            "interpretation": "Inflation persistence limits policy flexibility while growth cools.",
        },
        {
            "macro_path": "liquidity_sensitive_slowdown",
            "probability_proxy": round(
                min(
                    1.0,
                    0.45 * liquidity_pressure
                    + 0.30 * cb_pressure
                    + 0.25 * demand_pressure,
                ),
                4,
            ),
            "interpretation": "Liquidity conditions become increasingly important to macro stability.",
        },
        {
            "macro_path": "demand_fragility_acceleration",
            "probability_proxy": round(
                min(
                    1.0,
                    0.45 * demand_pressure
                    + 0.35 * growth_pressure
                    + 0.20 * liquidity_pressure,
                ),
                4,
            ),
            "interpretation": "Demand deterioration begins reinforcing growth fragility.",
        },
    ]

    parquet_path = out_dir / "macroecon_recursive_engine_v1.parquet"
    json_path = out_dir / "macroecon_recursive_engine_v1.json"
    summary_path = out_dir / "macroecon_recursive_engine_summary_v1.json"
    paths_path = out_dir / "macroecon_recursive_paths_v1.json"

    df.to_parquet(parquet_path, index=False)

    df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(paths_path, "w", encoding="utf-8") as f:
        json.dump(macro_paths, f, indent=2)

    summary = {
        "component": "macroecon_recursive_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "macroecon_recursive_pressure": macroecon_recursive_pressure,
        "cb_recursive_pressure": round(cb_pressure, 4),
        "cb_adjusted_macro_pressure": cb_adjusted_macro_pressure,
        "max_component_score": max_component_score,
        "macroecon_recursive_state": macroecon_recursive_state,
        "macro_regime_bias": macro_regime_bias,
        "macro_policy_tension": macro_policy_tension,
        "growth_fragility_pressure": round(growth_pressure, 4),
        "inflation_persistence_pressure": round(inflation_pressure, 4),
        "liquidity_cycle_pressure": round(liquidity_pressure, 4),
        "demand_deterioration_pressure": round(demand_pressure, 4),
        "iv_vector_targets": sorted(
            df["iv_vector"].unique().tolist()
        ),
        "active_components": df["component"].tolist(),
        "macro_paths": macro_paths,
        "status": "macroecon_recursive_engine_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Macroecon recursive engine complete")
    print("Rows:", len(df))
    print("Macroecon Recursive Pressure:", macroecon_recursive_pressure)
    print("CB-Adjusted Macro Pressure:", cb_adjusted_macro_pressure)
    print("Macroecon Recursive State:", macroecon_recursive_state)
    print("Macro Regime Bias:", macro_regime_bias)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("PATHS:", paths_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_macroecon_recursive_engine_v1()

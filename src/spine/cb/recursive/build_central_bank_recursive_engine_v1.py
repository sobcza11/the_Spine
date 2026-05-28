from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json
import numpy as np


def build_central_bank_recursive_engine_v1():

    repo_root = Path.cwd()

    out_dir = (
        repo_root
        / "data"
        / "cb"
        / "recursive"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # =====================================================
    # CENTRAL BANK RECURSIVE COMPONENTS
    # =====================================================

    cb_components = [

        {
            "component": "policy_constraint_pressure",
            "raw_score": 0.44,
            "weight": 0.24,
            "recursive_channel": "hawkish_feedback_loop",
            "iv_vector": "C",
        },

        {
            "component": "liquidity_stabilization_pressure",
            "raw_score": 0.39,
            "weight": 0.22,
            "recursive_channel": "liquidity_backstop",
            "iv_vector": "L",
        },

        {
            "component": "inflation_recursion_pressure",
            "raw_score": 0.42,
            "weight": 0.20,
            "recursive_channel": "inflation_feedback_cycle",
            "iv_vector": "F",
        },

        {
            "component": "growth_deterioration_pressure",
            "raw_score": 0.37,
            "weight": 0.18,
            "recursive_channel": "growth_fragility_loop",
            "iv_vector": "G",
        },

        {
            "component": "market_function_pressure",
            "raw_score": 0.34,
            "weight": 0.16,
            "recursive_channel": "market_stability_support",
            "iv_vector": "S",
        },
    ]

    df = pd.DataFrame(
        cb_components
    )

    df["weighted_score"] = (
        df["raw_score"]
        * df["weight"]
    )

    recursive_pressure = round(
        float(
            df["weighted_score"].sum()
        ),
        4,
    )

    max_component_score = round(
        float(
            df["raw_score"].max()
        ),
        4,
    )

    # =====================================================
    # POLICY RECURSIVE STATES
    # =====================================================

    if recursive_pressure >= 0.70:
        recursive_state = (
            "systemic_cb_recursion"
        )

    elif recursive_pressure >= 0.55:
        recursive_state = (
            "fragile_cb_recursion"
        )

    elif recursive_pressure >= 0.40:
        recursive_state = (
            "elevated_cb_recursion"
        )

    elif recursive_pressure >= 0.25:
        recursive_state = (
            "watch_cb_recursion"
        )

    else:
        recursive_state = (
            "stable_cb_recursion"
        )

    # =====================================================
    # POLICY STANCE CLASSIFICATION
    # =====================================================

    inflation_pressure = float(
        df.loc[
            df["component"]
            == "inflation_recursion_pressure",
            "raw_score",
        ].iloc[0]
    )

    growth_pressure = float(
        df.loc[
            df["component"]
            == "growth_deterioration_pressure",
            "raw_score",
        ].iloc[0]
    )

    liquidity_pressure = float(
        df.loc[
            df["component"]
            == "liquidity_stabilization_pressure",
            "raw_score",
        ].iloc[0]
    )

    if (
        inflation_pressure > growth_pressure
        and inflation_pressure >= 0.40
    ):

        dominant_cb_bias = (
            "hawkish_constraint"
        )

    elif liquidity_pressure >= 0.45:

        dominant_cb_bias = (
            "liquidity_stabilization"
        )

    else:

        dominant_cb_bias = (
            "balanced_policy_watch"
        )

    # =====================================================
    # RECURSIVE POLICY PATHS
    # =====================================================

    policy_paths = [

        {
            "policy_path": "higher_for_longer",
            "probability_proxy": round(
                inflation_pressure
                * 0.55,
                4,
            ),
            "interpretation":
            "Inflation persistence constrains easing.",
        },

        {
            "policy_path": "liquidity_support",
            "probability_proxy": round(
                liquidity_pressure
                * 0.60,
                4,
            ),
            "interpretation":
            "Liquidity stress increases intervention probability.",
        },

        {
            "policy_path": "growth_support_shift",
            "probability_proxy": round(
                growth_pressure
                * 0.50,
                4,
            ),
            "interpretation":
            "Growth deterioration pressures easing bias.",
        },
    ]

    # =====================================================
    # OUTPUTS
    # =====================================================

    parquet_path = (
        out_dir
        / "central_bank_recursive_engine_v1.parquet"
    )

    json_path = (
        out_dir
        / "central_bank_recursive_engine_v1.json"
    )

    summary_path = (
        out_dir
        / "central_bank_recursive_engine_summary_v1.json"
    )

    policy_path_json = (
        out_dir
        / "central_bank_recursive_policy_paths_v1.json"
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

    with open(
        policy_path_json,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            policy_paths,
            f,
            indent=2,
        )

    summary = {

        "component":
        "central_bank_recursive_engine_v1",

        "generated_at_utc":
        datetime.now(UTC).isoformat(),

        "component_count":
        int(len(df)),

        "central_bank_recursive_pressure":
        recursive_pressure,

        "max_component_score":
        max_component_score,

        "central_bank_recursive_state":
        recursive_state,

        "dominant_cb_bias":
        dominant_cb_bias,

        "inflation_recursion_pressure":
        round(
            inflation_pressure,
            4,
        ),

        "growth_deterioration_pressure":
        round(
            growth_pressure,
            4,
        ),

        "liquidity_stabilization_pressure":
        round(
            liquidity_pressure,
            4,
        ),

        "iv_vector_targets":
        sorted(
            df["iv_vector"]
            .unique()
            .tolist()
        ),

        "active_components":
        df["component"]
        .tolist(),

        "policy_paths":
        policy_paths,

        "status":
        "central_bank_recursive_engine_complete",
    }

    with open(
        summary_path,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            summary,
            f,
            indent=2,
        )

    print(
        "Central bank recursive engine complete"
    )

    print(
        "Rows:",
        len(df),
    )

    print(
        "Central Bank Recursive Pressure:",
        recursive_pressure,
    )

    print(
        "Central Bank Recursive State:",
        recursive_state,
    )

    print(
        "Dominant CB Bias:",
        dominant_cb_bias,
    )

    print(
        "PARQUET:",
        parquet_path,
    )

    print(
        "JSON:",
        json_path,
    )

    print(
        "SUMMARY:",
        summary_path,
    )

    print(
        "Summary:",
        summary,
    )

    return df


if __name__ == "__main__":

    build_central_bank_recursive_engine_v1()

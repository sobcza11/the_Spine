from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import json


CREDIT_COMPONENTS = [

    {
        "component": "credit_spread_stress",
        "raw_score": 0.43,
        "weight": 0.22,
        "recursive_channel": "spread_widening_loop",
        "iv_vector": "C",
    },

    {
        "component": "funding_liquidity_stress",
        "raw_score": 0.39,
        "weight": 0.20,
        "recursive_channel": "funding_fragility_loop",
        "iv_vector": "L",
    },

    {
        "component": "default_cycle_pressure",
        "raw_score": 0.36,
        "weight": 0.18,
        "recursive_channel": "default_feedback_loop",
        "iv_vector": "D",
    },

    {
        "component": "bank_balance_sheet_pressure",
        "raw_score": 0.37,
        "weight": 0.16,
        "recursive_channel": "balance_sheet_contraction_loop",
        "iv_vector": "B",
    },

    {
        "component": "corporate_refinancing_pressure",
        "raw_score": 0.41,
        "weight": 0.14,
        "recursive_channel": "refinancing_constraint_loop",
        "iv_vector": "R",
    },

    {
        "component": "credit_market_confidence_pressure",
        "raw_score": 0.40,
        "weight": 0.10,
        "recursive_channel": "confidence_fragility_loop",
        "iv_vector": "S",
    },
]


def classify_credit_state(score):

    if score >= 0.75:
        return "systemic_credit_recursion"

    elif score >= 0.60:
        return "fragile_credit_recursion"

    elif score >= 0.40:
        return "elevated_credit_recursion"

    elif score >= 0.25:
        return "watch_credit_recursion"

    return "stable_credit_recursion"


def load_json(path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_credit_recursive_topology_engine_v1():

    repo_root = Path.cwd()

    out_dir = (
        repo_root
        / "data"
        / "credit"
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

    macro_pressure = float(
        macro_summary.get(
            "cb_adjusted_macro_pressure",
            0.0,
        )
    )

    cb_pressure = float(
        cb_summary.get(
            "central_bank_recursive_pressure",
            0.0,
        )
    )

    macro_bias = macro_summary.get(
        "macro_regime_bias",
        "unknown",
    )

    df = pd.DataFrame(
        CREDIT_COMPONENTS
    )

    df["weighted_score"] = (
        df["raw_score"]
        * df["weight"]
    ).round(4)

    credit_recursive_pressure = round(
        float(
            df["weighted_score"].sum()
        ),
        4,
    )

    macro_adjusted_credit_pressure = round(
        min(
            1.0,
            (
                0.72
                * credit_recursive_pressure
            )
            + (
                0.18
                * macro_pressure
            )
            + (
                0.10
                * cb_pressure
            ),
        ),
        4,
    )

    max_component_score = round(
        float(
            df["raw_score"].max()
        ),
        4,
    )

    credit_recursive_state = (
        classify_credit_state(
            macro_adjusted_credit_pressure
        )
    )

    spread_pressure = float(
        df.loc[
            df["component"]
            == "credit_spread_stress",
            "raw_score",
        ].iloc[0]
    )

    funding_pressure = float(
        df.loc[
            df["component"]
            == "funding_liquidity_stress",
            "raw_score",
        ].iloc[0]
    )

    default_pressure = float(
        df.loc[
            df["component"]
            == "default_cycle_pressure",
            "raw_score",
        ].iloc[0]
    )

    refinancing_pressure = float(
        df.loc[
            df["component"]
            == "corporate_refinancing_pressure",
            "raw_score",
        ].iloc[0]
    )

    credit_reflexivity_score = round(
        (
            0.35
            * spread_pressure
        )
        + (
            0.30
            * funding_pressure
        )
        + (
            0.20
            * refinancing_pressure
        )
        + (
            0.15
            * default_pressure
        ),
        4,
    )

    if (
        macro_bias
        == "inflation_constrained_slowdown"
        and refinancing_pressure >= 0.40
    ):

        dominant_credit_regime = (
            "refinancing_fragility_regime"
        )

    elif funding_pressure >= 0.45:

        dominant_credit_regime = (
            "funding_stress_regime"
        )

    elif spread_pressure >= 0.45:

        dominant_credit_regime = (
            "spread_decompression_regime"
        )

    else:

        dominant_credit_regime = (
            "watch_credit_regime"
        )

    topology_links = [

        {
            "source": "macroecon",
            "target": "credit",
            "transmission_channel":
            "growth_fragility",
            "recursive_feedback":
            round(
                macro_pressure
                * 0.11,
                4,
            ),
        },

        {
            "source": "central_bank",
            "target": "credit",
            "transmission_channel":
            "policy_constraint",
            "recursive_feedback":
            round(
                cb_pressure
                * 0.10,
                4,
            ),
        },

        {
            "source": "credit",
            "target": "equities",
            "transmission_channel":
            "earnings_reflexivity",
            "recursive_feedback":
            round(
                credit_recursive_pressure
                * 0.12,
                4,
            ),
        },

        {
            "source": "credit",
            "target": "finstate",
            "transmission_channel":
            "balance_sheet_stress",
            "recursive_feedback":
            round(
                credit_recursive_pressure
                * 0.13,
                4,
            ),
        },
    ]

    credit_paths = [

        {
            "credit_path":
            "contained_credit_watch",

            "probability_proxy":
            round(
                max(
                    0.0,
                    1.0
                    - macro_adjusted_credit_pressure,
                ),
                4,
            ),

            "interpretation":
            "Credit conditions remain stressed but manageable.",
        },

        {
            "credit_path":
            "refinancing_fragility",

            "probability_proxy":
            round(
                (
                    0.45
                    * refinancing_pressure
                )
                + (
                    0.30
                    * macro_pressure
                )
                + (
                    0.25
                    * cb_pressure
                ),
                4,
            ),

            "interpretation":
            "Higher rates increase refinancing vulnerability.",
        },

        {
            "credit_path":
            "funding_stress_acceleration",

            "probability_proxy":
            round(
                (
                    0.45
                    * funding_pressure
                )
                + (
                    0.30
                    * spread_pressure
                )
                + (
                    0.25
                    * macro_pressure
                ),
                4,
            ),

            "interpretation":
            "Liquidity and funding stress reinforce recursively.",
        },

        {
            "credit_path":
            "default_cycle_expansion",

            "probability_proxy":
            round(
                (
                    0.40
                    * default_pressure
                )
                + (
                    0.35
                    * spread_pressure
                )
                + (
                    0.25
                    * refinancing_pressure
                ),
                4,
            ),

            "interpretation":
            "Credit deterioration begins broadening across sectors.",
        },
    ]

    parquet_path = (
        out_dir
        / "credit_recursive_topology_engine_v1.parquet"
    )

    json_path = (
        out_dir
        / "credit_recursive_topology_engine_v1.json"
    )

    topology_path = (
        out_dir
        / "credit_recursive_topology_links_v1.json"
    )

    paths_path = (
        out_dir
        / "credit_recursive_paths_v1.json"
    )

    summary_path = (
        out_dir
        / "credit_recursive_topology_engine_summary_v1.json"
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
        topology_path,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            topology_links,
            f,
            indent=2,
        )

    with open(
        paths_path,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            credit_paths,
            f,
            indent=2,
        )

    summary = {

        "component":
        "credit_recursive_topology_engine_v1",

        "generated_at_utc":
        datetime.now(UTC).isoformat(),

        "component_count":
        int(len(df)),

        "credit_recursive_pressure":
        credit_recursive_pressure,

        "macro_adjusted_credit_pressure":
        macro_adjusted_credit_pressure,

        "credit_recursive_state":
        credit_recursive_state,

        "dominant_credit_regime":
        dominant_credit_regime,

        "credit_reflexivity_score":
        credit_reflexivity_score,

        "macro_regime_bias":
        macro_bias,

        "macro_pressure":
        round(
            macro_pressure,
            4,
        ),

        "cb_pressure":
        round(
            cb_pressure,
            4,
        ),

        "max_component_score":
        max_component_score,

        "iv_vector_targets":
        sorted(
            df["iv_vector"]
            .unique()
            .tolist()
        ),

        "active_components":
        df["component"]
        .tolist(),

        "topology_links":
        topology_links,

        "credit_paths":
        credit_paths,

        "status":
        "credit_recursive_topology_complete",
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
        "Credit recursive topology engine complete"
    )

    print(
        "Rows:",
        len(df),
    )

    print(
        "Credit Recursive Pressure:",
        credit_recursive_pressure,
    )

    print(
        "Macro Adjusted Credit Pressure:",
        macro_adjusted_credit_pressure,
    )

    print(
        "Credit Recursive State:",
        credit_recursive_state,
    )

    print(
        "Dominant Credit Regime:",
        dominant_credit_regime,
    )

    print(
        "Credit Reflexivity Score:",
        credit_reflexivity_score,
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
        "TOPOLOGY:",
        topology_path,
    )

    print(
        "PATHS:",
        paths_path,
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

    build_credit_recursive_topology_engine_v1()

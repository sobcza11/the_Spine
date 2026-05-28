from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


SCENARIO_BRANCHES = [
    {
        "root_scenario": "baseline_watch",
        "branch": "soft_stabilization",
        "description": "Recursive pressure fades as policy credibility and liquidity expectations stabilize risk channels.",
        "pressure_multiplier": 0.75,
        "policy_multiplier": 0.80,
        "global_multiplier": 0.80,
    },
    {
        "root_scenario": "baseline_watch",
        "branch": "hawkish_constraint_persistence",
        "description": "Inflation constraints keep policy reactive, limiting growth support while recursive pressure persists.",
        "pressure_multiplier": 1.05,
        "policy_multiplier": 1.20,
        "global_multiplier": 1.00,
    },
    {
        "root_scenario": "fx_rates_feedback",
        "branch": "fx_to_rates_amplification",
        "description": "FX stress amplifies rates pressure, increasing duration sensitivity and cross-domain recursion.",
        "pressure_multiplier": 1.20,
        "policy_multiplier": 1.10,
        "global_multiplier": 1.05,
    },
    {
        "root_scenario": "rates_finstate_feedback",
        "branch": "rates_to_corporate_fragility",
        "description": "Rates pressure feeds into FinState fragility through funding, credit, and valuation channels.",
        "pressure_multiplier": 1.25,
        "policy_multiplier": 1.05,
        "global_multiplier": 1.10,
    },
    {
        "root_scenario": "global_bridge",
        "branch": "us_to_em_liquidity_stress",
        "description": "US policy and dollar liquidity pressure transmit into EM fragility channels.",
        "pressure_multiplier": 1.30,
        "policy_multiplier": 1.15,
        "global_multiplier": 1.25,
    },
    {
        "root_scenario": "cascade_tail",
        "branch": "recursive_contagion_acceleration",
        "description": "Recursive feedback and contagion reinforce each other, raising systemic transition risk.",
        "pressure_multiplier": 1.60,
        "policy_multiplier": 1.30,
        "global_multiplier": 1.35,
    },
]


def load_json(path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def classify_tree_state(score):
    if score >= 0.80:
        return "tree_cascade_risk"

    if score >= 0.65:
        return "tree_systemic_fragility"

    if score >= 0.50:
        return "tree_elevated_fragility"

    if score >= 0.35:
        return "tree_watch"

    return "tree_stable"


def build_recursive_scenario_tree_generation_v1():
    repo_root = Path.cwd()

    projection = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "projection"
        / "recursive_scenario_projection_summary_v1.json"
    )

    fusion = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "fusion"
        / "cross_domain_recursive_fusion_summary_v1.json"
    )

    policy = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "policy"
        / "recursive_policy_response_layer_summary_v1.json"
    )

    global_expansion = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "global_expansion"
        / "global_recursive_ae_em_expansion_summary_v1.json"
    )

    governance = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "geoscen_recursive_governance_summary_v1.json"
    )

    regime_memory = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "fusion"
        / "recursive_regime_memory_summary_v1.json"
    )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "scenario_tree"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    base_projection_pressure = float(
        projection.get("base_projection_pressure", 0.0) or 0.0
    )

    cross_domain_pressure = float(
        fusion.get("cross_domain_recursive_pressure", 0.0) or 0.0
    )

    policy_response_pressure = float(
        policy.get("policy_response_pressure", 0.0) or 0.0
    )

    global_recursive_pressure = float(
        global_expansion.get("global_recursive_pressure", 0.0) or 0.0
    )

    regime_memory_score = float(
        regime_memory.get("regime_memory_score", 0.0) or 0.0
    )

    governance_state = governance.get("governance_state", "unknown")

    governance_adjustment = 1.00

    if governance_state == "governance_watch":
        governance_adjustment = 0.95
    elif governance_state == "governance_throttle":
        governance_adjustment = 0.80
    elif governance_state == "governance_lockdown":
        governance_adjustment = 0.60

    base_tree_pressure = round(
        min(
            1.0,
            0.25 * base_projection_pressure
            + 0.22 * cross_domain_pressure
            + 0.20 * policy_response_pressure
            + 0.18 * global_recursive_pressure
            + 0.15 * regime_memory_score,
        ),
        4,
    )

    rows = []

    for item in SCENARIO_BRANCHES:
        projected_branch_pressure = min(
            1.0,
            base_tree_pressure
            * item["pressure_multiplier"]
            * governance_adjustment,
        )

        policy_adjusted_pressure = min(
            1.0,
            policy_response_pressure
            * item["policy_multiplier"]
            * governance_adjustment,
        )

        global_adjusted_pressure = min(
            1.0,
            global_recursive_pressure
            * item["global_multiplier"]
            * governance_adjustment,
        )

        branch_systemic_score = round(
            min(
                1.0,
                0.50 * projected_branch_pressure
                + 0.25 * policy_adjusted_pressure
                + 0.25 * global_adjusted_pressure,
            ),
            4,
        )

        branch_cascade_probability = round(
            min(
                1.0,
                0.55 * branch_systemic_score
                + 0.25 * projected_branch_pressure
                + 0.20 * global_adjusted_pressure,
            ),
            4,
        )

        rows.append(
            {
                "root_scenario": item["root_scenario"],
                "branch": item["branch"],
                "description": item["description"],
                "pressure_multiplier": item["pressure_multiplier"],
                "policy_multiplier": item["policy_multiplier"],
                "global_multiplier": item["global_multiplier"],
                "projected_branch_pressure": round(projected_branch_pressure, 4),
                "policy_adjusted_pressure": round(policy_adjusted_pressure, 4),
                "global_adjusted_pressure": round(global_adjusted_pressure, 4),
                "branch_systemic_score": branch_systemic_score,
                "branch_cascade_probability": branch_cascade_probability,
                "branch_state": classify_tree_state(branch_systemic_score),
                "governance_state": governance_state,
                "governance_adjustment": governance_adjustment,
            }
        )

    tree_df = pd.DataFrame(rows)

    summary = {
        "component": "recursive_scenario_tree_generation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "branch_count": int(len(tree_df)),
        "base_tree_pressure": base_tree_pressure,
        "governance_state": governance_state,
        "governance_adjustment": governance_adjustment,
        "max_branch_systemic_score": round(
            float(tree_df["branch_systemic_score"].max()),
            4,
        ),
        "max_branch_cascade_probability": round(
            float(tree_df["branch_cascade_probability"].max()),
            4,
        ),
        "highest_risk_branch": tree_df.sort_values(
            "branch_systemic_score",
            ascending=False,
        ).iloc[0]["branch"],
        "highest_risk_state": tree_df.sort_values(
            "branch_systemic_score",
            ascending=False,
        ).iloc[0]["branch_state"],
        "branch_state_counts": tree_df["branch_state"].value_counts().to_dict(),
        "status": "recursive_scenario_tree_generation_complete",
    }

    parquet_path = out_dir / "recursive_scenario_tree_generation_v1.parquet"
    json_path = out_dir / "recursive_scenario_tree_generation_v1.json"
    summary_path = out_dir / "recursive_scenario_tree_generation_summary_v1.json"

    tree_df.to_parquet(parquet_path, index=False)

    tree_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive scenario tree generation complete")
    print("Rows:", len(tree_df))
    print("Base Tree Pressure:", base_tree_pressure)
    print("Highest Risk Branch:", summary["highest_risk_branch"])
    print("Highest Risk State:", summary["highest_risk_state"])
    print("Max Branch Systemic Score:", summary["max_branch_systemic_score"])
    print("Max Branch Cascade Probability:", summary["max_branch_cascade_probability"])
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return tree_df


if __name__ == "__main__":
    build_recursive_scenario_tree_generation_v1()

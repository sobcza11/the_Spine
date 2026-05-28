from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


REGISTRY_LIMIT = 1000


def load_json(path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_runtime_state_registry_v1():

    repo_root = Path.cwd()

    visibility_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
    )

    cache_dir = (
        visibility_dir
        / "dashboard_cache"
    )

    registry_dir = (
        visibility_dir
        / "runtime_registry"
    )

    registry_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    snapshot = load_json(
        cache_dir
        / "recursive_dashboard_snapshot_v1.json"
    )

    existing_registry_path = (
        registry_dir
        / "runtime_state_registry_v1.json"
    )

    existing_history = []

    if existing_registry_path.exists():

        with open(
            existing_registry_path,
            "r",
            encoding="utf-8",
        ) as f:

            existing_history = json.load(f)

    dashboard_summary = snapshot.get(
        "dashboard_summary",
        {}
    )

    runtime = snapshot.get(
        "runtime",
        {}
    )

    systemic = runtime.get(
        "systemic_state",
        {}
    )

    policy = runtime.get(
        "policy_state",
        {}
    )

    scenario_tree = runtime.get(
        "scenario_tree_state",
        {}
    )

    governance = runtime.get(
        "governance_state",
        {}
    )

    executive = runtime.get(
        "executive_state",
        {}
    )

    overall_runtime = runtime.get(
        "overall_runtime_state",
        {}
    )

    registry_entry = {

        "timestamp_utc": datetime.now(UTC).isoformat(),

        # =============================================
        # RUNTIME
        # =============================================

        "runtime_health": overall_runtime.get(
            "runtime_health"
        ),

        "runtime_mode": overall_runtime.get(
            "runtime_mode"
        ),

        # =============================================
        # SYSTEMIC
        # =============================================

        "systemic_risk_level": overall_runtime.get(
            "systemic_risk_level"
        ),

        "fragility_state": systemic.get(
            "fragility_state"
        ),

        "fragility_score": systemic.get(
            "fragility_score"
        ),

        "cascade_probability": systemic.get(
            "cascade_probability"
        ),

        "recursive_contagion_state": systemic.get(
            "recursive_contagion_state"
        ),

        "cross_domain_recursive_state": systemic.get(
            "cross_domain_recursive_state"
        ),

        "cross_domain_recursive_pressure": systemic.get(
            "cross_domain_recursive_pressure"
        ),

        # =============================================
        # POLICY
        # =============================================

        "policy_response_state": policy.get(
            "policy_response_state"
        ),

        "policy_response_pressure": policy.get(
            "policy_response_pressure"
        ),

        "dominant_policy_bias": policy.get(
            "dominant_policy_bias"
        ),

        # =============================================
        # GLOBAL
        # =============================================

        "global_recursive_mode": overall_runtime.get(
            "global_recursive_mode"
        ),

        "global_recursive_pressure": dashboard_summary.get(
            "global_recursive_pressure"
        ),

        # =============================================
        # SCENARIO TREE
        # =============================================

        "highest_risk_branch": dashboard_summary.get(
            "highest_risk_branch"
        ),

        "highest_risk_state": scenario_tree.get(
            "highest_risk_state"
        ),

        "max_branch_systemic_score": scenario_tree.get(
            "max_branch_systemic_score"
        ),

        "max_branch_cascade_probability": scenario_tree.get(
            "max_branch_cascade_probability"
        ),

        # =============================================
        # GOVERNANCE
        # =============================================

        "governance_state": governance.get(
            "governance_state"
        ),

        "governance_action": governance.get(
            "governance_action"
        ),

        "recursion_mode": governance.get(
            "recursion_mode"
        ),

        # =============================================
        # EXECUTIVE
        # =============================================

        "management_view": executive.get(
            "management_view"
        ),

        # =============================================
        # PIPELINE
        # =============================================

        "pipeline_steps": dashboard_summary.get(
            "pipeline_steps"
        ),

        "successful_steps": dashboard_summary.get(
            "successful_steps"
        ),

        "failed_steps": dashboard_summary.get(
            "failed_steps"
        ),
    }

    existing_history.append(
        registry_entry
    )

    if len(existing_history) > REGISTRY_LIMIT:

        existing_history = existing_history[
            -REGISTRY_LIMIT:
        ]

    registry_df = pd.DataFrame(
        existing_history
    )

    # =================================================
    # DELTA ENGINE
    # =================================================

    if len(registry_df) >= 2:

        current = registry_df.iloc[-1]
        previous = registry_df.iloc[-2]

        fragility_delta = round(
            float(current["fragility_score"])
            - float(previous["fragility_score"]),
            4,
        )

        recursive_pressure_delta = round(
            float(current["cross_domain_recursive_pressure"])
            - float(previous["cross_domain_recursive_pressure"]),
            4,
        )

        cascade_delta = round(
            float(current["cascade_probability"])
            - float(previous["cascade_probability"]),
            4,
        )

    else:

        fragility_delta = 0.0
        recursive_pressure_delta = 0.0
        cascade_delta = 0.0

    summary = {
        "component": "runtime_state_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "registry_entries": int(len(registry_df)),
        "registry_limit": REGISTRY_LIMIT,
        "latest_runtime_health": registry_entry["runtime_health"],
        "latest_systemic_risk_level": registry_entry["systemic_risk_level"],
        "latest_global_recursive_mode": registry_entry["global_recursive_mode"],
        "latest_highest_risk_branch": registry_entry["highest_risk_branch"],
        "fragility_delta": fragility_delta,
        "recursive_pressure_delta": recursive_pressure_delta,
        "cascade_probability_delta": cascade_delta,
        "registry_status": "historical_runtime_tracking_active",
        "status": "runtime_state_registry_complete",
    }

    parquet_path = (
        registry_dir
        / "runtime_state_registry_v1.parquet"
    )

    json_path = (
        registry_dir
        / "runtime_state_registry_v1.json"
    )

    summary_path = (
        registry_dir
        / "runtime_state_registry_summary_v1.json"
    )

    registry_df.to_parquet(
        parquet_path,
        index=False,
    )

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            existing_history,
            f,
            indent=2,
        )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            indent=2,
        )

    print("Runtime state registry complete")
    print("Registry Entries:", len(registry_df))
    print("Latest Runtime Health:", summary["latest_runtime_health"])
    print("Latest Systemic Risk Level:", summary["latest_systemic_risk_level"])
    print("Latest Highest Risk Branch:", summary["latest_highest_risk_branch"])
    print("Fragility Delta:", fragility_delta)
    print("Recursive Pressure Delta:", recursive_pressure_delta)
    print("Cascade Probability Delta:", cascade_delta)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return registry_df


if __name__ == "__main__":
    build_runtime_state_registry_v1()

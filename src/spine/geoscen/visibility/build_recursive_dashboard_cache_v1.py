from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def load_json(path):
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def flatten_dict(d, parent_key="", sep="."):
    items = []

    for k, v in d.items():

        new_key = (
            f"{parent_key}{sep}{k}"
            if parent_key
            else k
        )

        if isinstance(v, dict):
            items.extend(
                flatten_dict(v, new_key, sep=sep).items()
            )

        else:
            items.append((new_key, v))

    return dict(items)


def build_recursive_dashboard_cache_v1():

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

    cache_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    runtime_status = load_json(
        visibility_dir
        / "recursive_status_runtime_v1.json"
    )

    orchestration = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "orchestration"
        / "recursive_geoscen_orchestration_summary_v1.json"
    )

    scenario_tree = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "scenario_tree"
        / "recursive_scenario_tree_generation_summary_v1.json"
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

    executive = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "executive"
        / "recursive_executive_synthesis_pack_v1.json"
    )

    snapshot = {
        "component": "recursive_dashboard_cache_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),

        "runtime": runtime_status,

        "dashboard_summary": {
            "runtime_health": runtime_status.get(
                "overall_runtime_state",
                {},
            ).get("runtime_health"),

            "systemic_risk_level": runtime_status.get(
                "overall_runtime_state",
                {},
            ).get("systemic_risk_level"),

            "global_recursive_mode": runtime_status.get(
                "overall_runtime_state",
                {},
            ).get("global_recursive_mode"),

            "policy_bias": policy.get(
                "dominant_policy_bias"
            ),

            "highest_risk_branch": scenario_tree.get(
                "highest_risk_branch"
            ),

            "highest_risk_state": scenario_tree.get(
                "highest_risk_state"
            ),

            "global_recursive_pressure": global_expansion.get(
                "global_recursive_pressure"
            ),

            "executive_read": executive.get(
                "interpretation",
                {},
            ).get("executive_read"),
        },

        "cache_metadata": {
            "cache_type": "offline_recursive_snapshot",
            "cache_scope": "full_recursive_runtime",
            "runtime_mode": "offline_recursive_monitoring",
            "orchestration_status": orchestration.get(
                "overall_status"
            ),
            "pipeline_steps": orchestration.get(
                "pipeline_steps"
            ),
            "successful_steps": orchestration.get(
                "successful_steps"
            ),
            "failed_steps": orchestration.get(
                "failed_steps"
            ),
        },
    }

    flat_snapshot = flatten_dict(snapshot)

    snapshot_df = pd.DataFrame(
        [flat_snapshot]
    )

    parquet_path = (
        cache_dir
        / "recursive_dashboard_snapshot_v1.parquet"
    )

    json_path = (
        cache_dir
        / "recursive_dashboard_snapshot_v1.json"
    )

    flat_json_path = (
        cache_dir
        / "recursive_dashboard_snapshot_flat_v1.json"
    )

    summary_path = (
        cache_dir
        / "recursive_dashboard_cache_summary_v1.json"
    )

    snapshot_df.to_parquet(
        parquet_path,
        index=False,
    )

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            snapshot,
            f,
            indent=2,
        )

    with open(flat_json_path, "w", encoding="utf-8") as f:
        json.dump(
            flat_snapshot,
            f,
            indent=2,
        )

    summary = {
        "component": "recursive_dashboard_cache_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "cache_status": "offline_dashboard_cache_active",
        "runtime_health": snapshot["dashboard_summary"]["runtime_health"],
        "systemic_risk_level": snapshot["dashboard_summary"]["systemic_risk_level"],
        "global_recursive_mode": snapshot["dashboard_summary"]["global_recursive_mode"],
        "highest_risk_branch": snapshot["dashboard_summary"]["highest_risk_branch"],
        "pipeline_steps": orchestration.get("pipeline_steps"),
        "successful_steps": orchestration.get("successful_steps"),
        "failed_steps": orchestration.get("failed_steps"),
        "status": "recursive_dashboard_cache_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            indent=2,
        )

    print("Recursive dashboard cache complete")
    print("Runtime Health:", summary["runtime_health"])
    print("Systemic Risk Level:", summary["systemic_risk_level"])
    print("Global Recursive Mode:", summary["global_recursive_mode"])
    print("Highest Risk Branch:", summary["highest_risk_branch"])
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("FLAT JSON:", flat_json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return snapshot_df


if __name__ == "__main__":
    build_recursive_dashboard_cache_v1()

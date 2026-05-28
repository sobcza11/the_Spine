from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


def classify_fragility_state(score):
    if score >= 0.85:
        return "cascade-risk"

    if score >= 0.70:
        return "systemic"

    if score >= 0.55:
        return "fragile"

    if score >= 0.40:
        return "elevated"

    if score >= 0.25:
        return "watch"

    return "stable"


def build_systemic_fragility_state_machine_v1():
    repo_root = Path.cwd()

    registry_summary_path = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "geoscen_systemic_escalation_registry_summary_v1.json"
    )

    recursive_summary_path = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
        / "recursive_escalation_engine_summary_v1.json"
    )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    with open(registry_summary_path, "r", encoding="utf-8") as f:
        registry_summary = json.load(f)

    with open(recursive_summary_path, "r", encoding="utf-8") as f:
        recursive_summary = json.load(f)

    systemic_escalation_score = float(
        registry_summary.get("systemic_escalation_score", 0.0) or 0.0
    )

    max_engine_score = float(
        registry_summary.get("max_engine_score", 0.0) or 0.0
    )

    recursive_escalation_pressure = float(
        recursive_summary.get("recursive_escalation_pressure", 0.0) or 0.0
    )

    avg_adjusted_target_pressure = float(
        recursive_summary.get("avg_adjusted_target_pressure", 0.0) or 0.0
    )

    max_adjusted_target_pressure = float(
        recursive_summary.get("max_adjusted_target_pressure", 0.0) or 0.0
    )

    fragility_score = (
        0.35 * systemic_escalation_score
        + 0.20 * max_engine_score
        + 0.20 * avg_adjusted_target_pressure
        + 0.15 * max_adjusted_target_pressure
        + 0.10 * recursive_escalation_pressure
    )

    fragility_score = round(min(1.0, fragility_score), 4)

    fragility_state = classify_fragility_state(fragility_score)

    cascade_probability = round(
        min(
            1.0,
            0.45 * fragility_score
            + 0.35 * max_adjusted_target_pressure
            + 0.20 * recursive_escalation_pressure,
        ),
        4,
    )

    state_rows = [
        {
            "state": "stable",
            "lower_bound": 0.00,
            "upper_bound": 0.25,
            "interpretation": "Systemic pressure is contained.",
        },
        {
            "state": "watch",
            "lower_bound": 0.25,
            "upper_bound": 0.40,
            "interpretation": "Early systemic pressure is present.",
        },
        {
            "state": "elevated",
            "lower_bound": 0.40,
            "upper_bound": 0.55,
            "interpretation": "Cross-engine escalation is building.",
        },
        {
            "state": "fragile",
            "lower_bound": 0.55,
            "upper_bound": 0.70,
            "interpretation": "System shows broad fragility.",
        },
        {
            "state": "systemic",
            "lower_bound": 0.70,
            "upper_bound": 0.85,
            "interpretation": "Systemic instability is active.",
        },
        {
            "state": "cascade-risk",
            "lower_bound": 0.85,
            "upper_bound": 1.00,
            "interpretation": "Recursive cascade risk is elevated.",
        },
    ]

    state_table = pd.DataFrame(state_rows)

    current_state = {
        "component": "systemic_fragility_state_machine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "systemic_escalation_score": round(systemic_escalation_score, 4),
        "max_engine_score": round(max_engine_score, 4),
        "recursive_escalation_pressure": round(recursive_escalation_pressure, 4),
        "avg_adjusted_target_pressure": round(avg_adjusted_target_pressure, 4),
        "max_adjusted_target_pressure": round(max_adjusted_target_pressure, 4),
        "systemic_fragility_score": fragility_score,
        "systemic_fragility_state": fragility_state,
        "cascade_probability": cascade_probability,
        "state_machine": state_rows,
        "status": "systemic_fragility_state_machine_complete",
    }

    parquet_path = out_dir / "systemic_fragility_state_machine_v1.parquet"
    json_path = out_dir / "systemic_fragility_state_machine_v1.json"
    summary_path = out_dir / "systemic_fragility_state_machine_summary_v1.json"

    state_table.to_parquet(parquet_path, index=False)

    state_table.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(current_state, f, indent=2)

    print("Systemic fragility state machine complete")
    print("Fragility Score:", fragility_score)
    print("Fragility State:", fragility_state)
    print("Cascade Probability:", cascade_probability)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", current_state)

    return current_state


if __name__ == "__main__":
    build_systemic_fragility_state_machine_v1()

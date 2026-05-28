from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


MEMORY_DECAY = 0.85


def classify_persistence(score):
    if score >= 0.75:
        return "persistent_systemic_memory"

    if score >= 0.60:
        return "persistent_fragility_memory"

    if score >= 0.40:
        return "building_memory"

    if score >= 0.25:
        return "early_memory"

    return "low_memory"


def build_recursive_topology_memory_engine_v1():
    repo_root = Path.cwd()

    recursive_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "recursive"
    )

    registry_summary_path = recursive_dir / "geoscen_systemic_escalation_registry_summary_v1.json"
    recursive_summary_path = recursive_dir / "recursive_escalation_engine_summary_v1.json"
    fragility_summary_path = recursive_dir / "systemic_fragility_state_machine_summary_v1.json"

    memory_path = recursive_dir / "recursive_topology_memory_state_v1.json"

    with open(registry_summary_path, "r", encoding="utf-8") as f:
        registry_summary = json.load(f)

    with open(recursive_summary_path, "r", encoding="utf-8") as f:
        recursive_summary = json.load(f)

    with open(fragility_summary_path, "r", encoding="utf-8") as f:
        fragility_summary = json.load(f)

    current_inputs = {
        "systemic_escalation_score": float(
            registry_summary.get("systemic_escalation_score", 0.0) or 0.0
        ),
        "recursive_escalation_pressure": float(
            recursive_summary.get("recursive_escalation_pressure", 0.0) or 0.0
        ),
        "avg_adjusted_target_pressure": float(
            recursive_summary.get("avg_adjusted_target_pressure", 0.0) or 0.0
        ),
        "systemic_fragility_score": float(
            fragility_summary.get("systemic_fragility_score", 0.0) or 0.0
        ),
        "cascade_probability": float(
            fragility_summary.get("cascade_probability", 0.0) or 0.0
        ),
    }

    current_memory_score = (
        0.25 * current_inputs["systemic_escalation_score"]
        + 0.15 * current_inputs["recursive_escalation_pressure"]
        + 0.20 * current_inputs["avg_adjusted_target_pressure"]
        + 0.25 * current_inputs["systemic_fragility_score"]
        + 0.15 * current_inputs["cascade_probability"]
    )

    current_memory_score = round(min(1.0, current_memory_score), 4)

    if memory_path.exists():
        with open(memory_path, "r", encoding="utf-8") as f:
            previous_memory = json.load(f)
    else:
        previous_memory = {
            "memory_score": 0.0,
            "memory_run_count": 0,
            "memory_state": "new_memory",
            "history": [],
        }

    previous_score = float(previous_memory.get("memory_score", 0.0) or 0.0)

    updated_memory_score = round(
        min(
            1.0,
            MEMORY_DECAY * previous_score
            + (1 - MEMORY_DECAY) * current_memory_score,
        ),
        4,
    )

    memory_run_count = int(previous_memory.get("memory_run_count", 0)) + 1

    memory_state = classify_persistence(updated_memory_score)

    history = previous_memory.get("history", [])

    history.append(
        {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "current_memory_score": current_memory_score,
            "updated_memory_score": updated_memory_score,
            "systemic_escalation_score": current_inputs["systemic_escalation_score"],
            "systemic_fragility_score": current_inputs["systemic_fragility_score"],
            "cascade_probability": current_inputs["cascade_probability"],
            "memory_state": memory_state,
        }
    )

    history = history[-52:]

    memory_output = {
        "component": "recursive_topology_memory_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "memory_decay": MEMORY_DECAY,
        "memory_run_count": memory_run_count,
        "current_memory_score": current_memory_score,
        "previous_memory_score": round(previous_score, 4),
        "memory_score": updated_memory_score,
        "memory_state": memory_state,
        "current_inputs": current_inputs,
        "history": history,
        "status": "recursive_topology_memory_complete",
    }

    summary_path = recursive_dir / "recursive_topology_memory_summary_v1.json"
    json_path = recursive_dir / "recursive_topology_memory_v1.json"
    parquet_path = recursive_dir / "recursive_topology_memory_v1.parquet"

    with open(memory_path, "w", encoding="utf-8") as f:
        json.dump(memory_output, f, indent=2)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(memory_output, f, indent=2)

    memory_df = pd.DataFrame(history)

    memory_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    memory_df.to_parquet(
        parquet_path,
        index=False,
    )

    print("Recursive topology memory engine complete")
    print("Current Memory Score:", current_memory_score)
    print("Previous Memory Score:", round(previous_score, 4))
    print("Updated Memory Score:", updated_memory_score)
    print("Memory State:", memory_state)
    print("Run Count:", memory_run_count)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("STATE:", memory_path)
    print("Summary:", memory_output)

    return memory_output


if __name__ == "__main__":
    build_recursive_topology_memory_engine_v1()

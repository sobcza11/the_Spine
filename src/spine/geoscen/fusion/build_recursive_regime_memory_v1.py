from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


MEMORY_DECAY = 0.88
HISTORY_LIMIT = 104


def classify_regime_memory(score):
    if score >= 0.80:
        return "persistent_systemic_regime"

    if score >= 0.65:
        return "persistent_fragile_regime"

    if score >= 0.50:
        return "persistent_elevated_regime"

    if score >= 0.35:
        return "building_watch_regime"

    if score >= 0.20:
        return "early_regime_memory"

    return "low_regime_memory"


def build_recursive_regime_memory_v1():
    repo_root = Path.cwd()

    fusion_dir = repo_root / "data" / "geoscen" / "fusion"
    recursive_dir = repo_root / "data" / "geoscen" / "recursive"

    out_dir = fusion_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    fusion_summary_path = fusion_dir / "cross_domain_recursive_fusion_summary_v1.json"
    fragility_summary_path = recursive_dir / "systemic_fragility_state_machine_summary_v1.json"
    topology_summary_path = recursive_dir / "recursive_geoscen_topology_summary_v1.json"
    governance_summary_path = recursive_dir / "geoscen_recursive_governance_summary_v1.json"

    memory_state_path = out_dir / "recursive_regime_memory_state_v1.json"

    with open(fusion_summary_path, "r", encoding="utf-8") as f:
        fusion = json.load(f)

    with open(fragility_summary_path, "r", encoding="utf-8") as f:
        fragility = json.load(f)

    with open(topology_summary_path, "r", encoding="utf-8") as f:
        topology = json.load(f)

    with open(governance_summary_path, "r", encoding="utf-8") as f:
        governance = json.load(f)

    cross_domain_recursive_pressure = float(
        fusion.get("cross_domain_recursive_pressure", 0.0) or 0.0
    )

    avg_adjusted_target_pressure = float(
        fusion.get("avg_adjusted_target_pressure", 0.0) or 0.0
    )

    max_adjusted_target_pressure = float(
        fusion.get("max_adjusted_target_pressure", 0.0) or 0.0
    )

    systemic_fragility_score = float(
        fragility.get("systemic_fragility_score", 0.0) or 0.0
    )

    cascade_probability = float(
        fragility.get("cascade_probability", 0.0) or 0.0
    )

    recursive_topology_score = float(
        topology.get("recursive_topology_score", 0.0) or 0.0
    )

    governance_pressure = float(
        governance.get("governance_pressure", 0.0) or 0.0
    )

    current_regime_memory_input = round(
        min(
            1.0,
            0.22 * cross_domain_recursive_pressure
            + 0.18 * avg_adjusted_target_pressure
            + 0.16 * max_adjusted_target_pressure
            + 0.16 * systemic_fragility_score
            + 0.12 * cascade_probability
            + 0.10 * recursive_topology_score
            + 0.06 * governance_pressure,
        ),
        4,
    )

    if memory_state_path.exists():
        with open(memory_state_path, "r", encoding="utf-8") as f:
            previous_memory = json.load(f)
    else:
        previous_memory = {
            "regime_memory_score": 0.0,
            "regime_memory_run_count": 0,
            "history": [],
        }

    previous_score = float(
        previous_memory.get("regime_memory_score", 0.0) or 0.0
    )

    updated_regime_memory_score = round(
        min(
            1.0,
            MEMORY_DECAY * previous_score
            + (1 - MEMORY_DECAY) * current_regime_memory_input,
        ),
        4,
    )

    regime_memory_run_count = int(
        previous_memory.get("regime_memory_run_count", 0)
    ) + 1

    regime_memory_state = classify_regime_memory(
        updated_regime_memory_score
    )

    current_snapshot = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "current_regime_memory_input": current_regime_memory_input,
        "previous_regime_memory_score": round(previous_score, 4),
        "regime_memory_score": updated_regime_memory_score,
        "regime_memory_state": regime_memory_state,
        "cross_domain_recursive_pressure": round(cross_domain_recursive_pressure, 4),
        "avg_adjusted_target_pressure": round(avg_adjusted_target_pressure, 4),
        "max_adjusted_target_pressure": round(max_adjusted_target_pressure, 4),
        "systemic_fragility_score": round(systemic_fragility_score, 4),
        "cascade_probability": round(cascade_probability, 4),
        "recursive_topology_score": round(recursive_topology_score, 4),
        "governance_pressure": round(governance_pressure, 4),
    }

    history = previous_memory.get("history", [])
    history.append(current_snapshot)
    history = history[-HISTORY_LIMIT:]

    output = {
        "component": "recursive_regime_memory_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "memory_decay": MEMORY_DECAY,
        "history_limit": HISTORY_LIMIT,
        "regime_memory_run_count": regime_memory_run_count,
        "current_regime_memory_input": current_regime_memory_input,
        "previous_regime_memory_score": round(previous_score, 4),
        "regime_memory_score": updated_regime_memory_score,
        "regime_memory_state": regime_memory_state,
        "history": history,
        "status": "recursive_regime_memory_complete",
    }

    summary_path = out_dir / "recursive_regime_memory_summary_v1.json"
    json_path = out_dir / "recursive_regime_memory_v1.json"
    parquet_path = out_dir / "recursive_regime_memory_v1.parquet"

    with open(memory_state_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    history_df = pd.DataFrame(history)

    history_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    history_df.to_parquet(
        parquet_path,
        index=False,
    )

    print("Recursive regime memory complete")
    print("Current Regime Memory Input:", current_regime_memory_input)
    print("Previous Regime Memory Score:", round(previous_score, 4))
    print("Updated Regime Memory Score:", updated_regime_memory_score)
    print("Regime Memory State:", regime_memory_state)
    print("Run Count:", regime_memory_run_count)
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("STATE:", memory_state_path)
    print("Summary:", output)

    return output


if __name__ == "__main__":
    build_recursive_regime_memory_v1()

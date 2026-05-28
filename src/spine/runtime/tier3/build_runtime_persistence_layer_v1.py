from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["runtime_state_checkpointing", 0.43, 0.26],
    ["persistent_execution_memory", 0.41, 0.22],
    ["runtime_snapshot_store", 0.40, 0.20],
    ["state_rehydration_contract", 0.42, 0.18],
    ["persistence_governance_control", 0.39, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_runtime_persistence"
    if x >= 0.55: return "fragile_runtime_persistence"
    if x >= 0.40: return "elevated_runtime_persistence"
    if x >= 0.25: return "watch_runtime_persistence"
    return "stable_runtime_persistence"

def build_runtime_persistence_layer_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "runtime_persistence_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "runtime_persistence_pressure": pressure,
        "runtime_persistence_state": state,
        "active_components": df["component"].tolist(),
        "status": "runtime_persistence_layer_complete",
    }

    df.to_parquet(out / "runtime_persistence_layer_v1.parquet", index=False)
    df.to_json(out / "runtime_persistence_layer_v1.json", orient="records", indent=2)

    with open(out / "runtime_persistence_layer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Runtime Persistence Layer complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "runtime_persistence_layer_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_runtime_persistence_layer_v1()

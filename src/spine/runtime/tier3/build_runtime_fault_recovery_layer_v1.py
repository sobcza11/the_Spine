from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["runtime_failure_detection", 0.43, 0.24],
    ["state_recovery_execution", 0.41, 0.22],
    ["checkpoint_rehydration", 0.40, 0.20],
    ["runtime_failover_control", 0.42, 0.18],
    ["recovery_governance_layer", 0.39, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_runtime_fault_recovery"
    if x >= 0.55: return "fragile_runtime_fault_recovery"
    if x >= 0.40: return "elevated_runtime_fault_recovery"
    if x >= 0.25: return "watch_runtime_fault_recovery"
    return "stable_runtime_fault_recovery"

def build_runtime_fault_recovery_layer_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "runtime_fault_recovery_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "runtime_fault_recovery_pressure": pressure,
        "runtime_fault_recovery_state": state,
        "active_components": df["component"].tolist(),
        "status": "runtime_fault_recovery_layer_complete",
    }

    df.to_parquet(out / "runtime_fault_recovery_layer_v1.parquet", index=False)
    df.to_json(out / "runtime_fault_recovery_layer_v1.json", orient="records", indent=2)

    with open(out / "runtime_fault_recovery_layer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Runtime Fault Recovery Layer complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_runtime_fault_recovery_layer_v1()

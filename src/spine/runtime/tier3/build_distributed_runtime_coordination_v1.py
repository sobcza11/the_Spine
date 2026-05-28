from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["distributed_runtime_sync", 0.43, 0.24],
    ["multi_node_state_alignment", 0.41, 0.22],
    ["runtime_coordination_router", 0.40, 0.20],
    ["cross_runtime_signal_transport", 0.42, 0.18],
    ["distributed_governance_control", 0.39, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_distributed_coordination"
    if x >= 0.55: return "fragile_distributed_coordination"
    if x >= 0.40: return "elevated_distributed_coordination"
    if x >= 0.25: return "watch_distributed_coordination"
    return "stable_distributed_coordination"

def build_distributed_runtime_coordination_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "distributed_runtime_coordination_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "distributed_runtime_coordination_pressure": pressure,
        "distributed_runtime_coordination_state": state,
        "active_components": df["component"].tolist(),
        "status": "distributed_runtime_coordination_complete",
    }

    df.to_parquet(out / "distributed_runtime_coordination_v1.parquet", index=False)
    df.to_json(out / "distributed_runtime_coordination_v1.json", orient="records", indent=2)

    with open(out / "distributed_runtime_coordination_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Distributed Runtime Coordination complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_distributed_runtime_coordination_v1()

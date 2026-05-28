from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["mesh_runtime_topology", 0.42, 0.24],
    ["distributed_signal_mesh", 0.41, 0.22],
    ["cross_node_runtime_transport", 0.40, 0.20],
    ["recursive_mesh_reconciliation", 0.39, 0.18],
    ["mesh_governance_boundary", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_mesh_runtime"
    if x >= 0.55: return "fragile_mesh_runtime"
    if x >= 0.40: return "elevated_mesh_runtime"
    if x >= 0.25: return "watch_mesh_runtime"
    return "stable_mesh_runtime"

def build_distributed_recursive_mesh_runtime_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "distributed_recursive_mesh_runtime_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "distributed_recursive_mesh_runtime_pressure": pressure,
        "distributed_recursive_mesh_runtime_state": state,
        "active_components": df["component"].tolist(),
        "status": "distributed_recursive_mesh_runtime_complete",
    }

    df.to_parquet(out / "distributed_recursive_mesh_runtime_v1.parquet", index=False)
    df.to_json(out / "distributed_recursive_mesh_runtime_v1.json", orient="records", indent=2)

    with open(out / "distributed_recursive_mesh_runtime_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Distributed Recursive Mesh Runtime complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_distributed_recursive_mesh_runtime_v1()

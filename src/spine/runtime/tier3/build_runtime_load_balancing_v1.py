from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["runtime_workload_distribution", 0.43, 0.24],
    ["resource_pressure_balancing", 0.41, 0.22],
    ["execution_capacity_routing", 0.40, 0.20],
    ["runtime_scaling_control", 0.42, 0.18],
    ["load_balancing_governance", 0.39, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_runtime_load_balancing"
    if x >= 0.55: return "fragile_runtime_load_balancing"
    if x >= 0.40: return "elevated_runtime_load_balancing"
    if x >= 0.25: return "watch_runtime_load_balancing"
    return "stable_runtime_load_balancing"

def build_runtime_load_balancing_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "runtime_load_balancing_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "runtime_load_balancing_pressure": pressure,
        "runtime_load_balancing_state": state,
        "active_components": df["component"].tolist(),
        "status": "runtime_load_balancing_complete",
    }

    df.to_parquet(out / "runtime_load_balancing_v1.parquet", index=False)
    df.to_json(out / "runtime_load_balancing_v1.json", orient="records", indent=2)

    with open(out / "runtime_load_balancing_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Runtime Load Balancing complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_runtime_load_balancing_v1()

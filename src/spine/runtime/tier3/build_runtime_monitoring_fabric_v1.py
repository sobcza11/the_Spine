from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["runtime_health_monitoring", 0.42, 0.24],
    ["recursive_metric_collection", 0.41, 0.22],
    ["execution_observability_layer", 0.40, 0.20],
    ["runtime_alert_visibility", 0.39, 0.18],
    ["monitoring_governance_filter", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_runtime_monitoring"
    if x >= 0.55: return "fragile_runtime_monitoring"
    if x >= 0.40: return "elevated_runtime_monitoring"
    if x >= 0.25: return "watch_runtime_monitoring"
    return "stable_runtime_monitoring"

def build_runtime_monitoring_fabric_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "runtime_monitoring_fabric_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "runtime_monitoring_fabric_pressure": pressure,
        "runtime_monitoring_fabric_state": state,
        "active_components": df["component"].tolist(),
        "status": "runtime_monitoring_fabric_complete",
    }

    df.to_parquet(out / "runtime_monitoring_fabric_v1.parquet", index=False)
    df.to_json(out / "runtime_monitoring_fabric_v1.json", orient="records", indent=2)

    with open(out / "runtime_monitoring_fabric_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Runtime Monitoring Fabric complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_runtime_monitoring_fabric_v1()

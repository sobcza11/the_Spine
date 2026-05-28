from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["runtime_performance_analytics", 0.42, 0.24],
    ["recursive_usage_statistics", 0.41, 0.22],
    ["operator_behavior_metrics", 0.40, 0.20],
    ["runtime_efficiency_tracking", 0.39, 0.18],
    ["analytics_governance_control", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_runtime_analytics"
    if x >= 0.55: return "fragile_runtime_analytics"
    if x >= 0.40: return "elevated_runtime_analytics"
    if x >= 0.25: return "watch_runtime_analytics"
    return "stable_runtime_analytics"

def build_recursive_runtime_analytics_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_runtime_analytics_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_runtime_analytics_pressure": pressure,
        "recursive_runtime_analytics_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_runtime_analytics_complete",
    }

    df.to_parquet(out / "recursive_runtime_analytics_v1.parquet", index=False)
    df.to_json(out / "recursive_runtime_analytics_v1.json", orient="records", indent=2)

    with open(out / "recursive_runtime_analytics_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Runtime Analytics complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_recursive_runtime_analytics_v1()

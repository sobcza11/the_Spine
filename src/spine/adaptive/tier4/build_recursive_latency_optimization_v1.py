from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["latency_measurement_layer", 0.42, 0.24],
    ["runtime_latency_reduction", 0.41, 0.22],
    ["adaptive_signal_prioritization", 0.40, 0.20],
    ["transport_efficiency_mapping", 0.39, 0.18],
    ["latency_governance_control", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_latency_optimization"
    if x >= 0.55: return "fragile_latency_optimization"
    if x >= 0.40: return "elevated_latency_optimization"
    if x >= 0.25: return "watch_latency_optimization"
    return "stable_latency_optimization"

def build_recursive_latency_optimization_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_latency_optimization_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_latency_optimization_pressure": pressure,
        "recursive_latency_optimization_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_latency_optimization_complete",
    }

    df.to_parquet(out / "recursive_latency_optimization_v1.parquet", index=False)
    df.to_json(out / "recursive_latency_optimization_v1.json", orient="records", indent=2)

    with open(out / "recursive_latency_optimization_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Latency Optimization complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_recursive_latency_optimization_v1()

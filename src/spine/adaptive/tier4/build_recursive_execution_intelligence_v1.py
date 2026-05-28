from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["execution_path_optimization", 0.42, 0.24],
    ["adaptive_order_routing", 0.41, 0.22],
    ["execution_latency_awareness", 0.40, 0.20],
    ["runtime_execution_feedback", 0.39, 0.18],
    ["execution_governance_layer", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_execution_intelligence"
    if x >= 0.55: return "fragile_execution_intelligence"
    if x >= 0.40: return "elevated_execution_intelligence"
    if x >= 0.25: return "watch_execution_intelligence"
    return "stable_execution_intelligence"

def build_recursive_execution_intelligence_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_execution_intelligence_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_execution_intelligence_pressure": pressure,
        "recursive_execution_intelligence_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_execution_intelligence_complete",
    }

    df.to_parquet(out / "recursive_execution_intelligence_v1.parquet", index=False)
    df.to_json(out / "recursive_execution_intelligence_v1.json", orient="records", indent=2)

    with open(out / "recursive_execution_intelligence_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Execution Intelligence complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_recursive_execution_intelligence_v1()

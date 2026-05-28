from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["allocation_drift_measurement", 0.42, 0.24],
    ["portfolio_deviation_mapping", 0.41, 0.22],
    ["recursive_weight_shift_detection", 0.40, 0.20],
    ["cross_asset_drift_alignment", 0.39, 0.18],
    ["allocation_governance_filter", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_allocation_drift"
    if x >= 0.55: return "fragile_allocation_drift"
    if x >= 0.40: return "elevated_allocation_drift"
    if x >= 0.25: return "watch_allocation_drift"
    return "stable_allocation_drift"

def build_recursive_allocation_drift_engine_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_allocation_drift_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_allocation_drift_pressure": pressure,
        "recursive_allocation_drift_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_allocation_drift_engine_complete",
    }

    df.to_parquet(out / "recursive_allocation_drift_engine_v1.parquet", index=False)
    df.to_json(out / "recursive_allocation_drift_engine_v1.json", orient="records", indent=2)

    with open(out / "recursive_allocation_drift_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Allocation Drift Engine complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_recursive_allocation_drift_engine_v1()

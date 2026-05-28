from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["recursive_risk_budgeting", 0.42, 0.24],
    ["cross_asset_risk_allocation", 0.41, 0.22],
    ["regime_adjusted_risk_routing", 0.40, 0.20],
    ["portfolio_risk_pressure_map", 0.39, 0.18],
    ["risk_allocation_governance", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_recursive_risk_allocation"
    if x >= 0.55: return "fragile_recursive_risk_allocation"
    if x >= 0.40: return "elevated_recursive_risk_allocation"
    if x >= 0.25: return "watch_recursive_risk_allocation"
    return "stable_recursive_risk_allocation"

def build_recursive_risk_allocation_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_risk_allocation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_risk_allocation_pressure": pressure,
        "recursive_risk_allocation_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_risk_allocation_complete",
    }

    df.to_parquet(out / "recursive_risk_allocation_v1.parquet", index=False)
    df.to_json(out / "recursive_risk_allocation_v1.json", orient="records", indent=2)

    with open(out / "recursive_risk_allocation_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Risk Allocation complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_risk_allocation_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_risk_allocation_v1()

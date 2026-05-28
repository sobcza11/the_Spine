from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["cross_market_capital_tracking", 0.42, 0.24],
    ["recursive_flow_pressure_mapping", 0.41, 0.22],
    ["institutional_flow_detection", 0.40, 0.20],
    ["macro_liquidity_alignment", 0.39, 0.18],
    ["capital_flow_governance", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_capital_flow_intelligence"
    if x >= 0.55: return "fragile_capital_flow_intelligence"
    if x >= 0.40: return "elevated_capital_flow_intelligence"
    if x >= 0.25: return "watch_capital_flow_intelligence"
    return "stable_capital_flow_intelligence"

def build_recursive_capital_flow_intelligence_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_capital_flow_intelligence_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_capital_flow_intelligence_pressure": pressure,
        "recursive_capital_flow_intelligence_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_capital_flow_intelligence_complete",
    }

    df.to_parquet(out / "recursive_capital_flow_intelligence_v1.parquet", index=False)
    df.to_json(out / "recursive_capital_flow_intelligence_v1.json", orient="records", indent=2)

    with open(out / "recursive_capital_flow_intelligence_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Capital Flow Intelligence complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_recursive_capital_flow_intelligence_v1()

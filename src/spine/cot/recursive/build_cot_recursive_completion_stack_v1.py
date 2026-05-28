from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["dealer_positioning_recursion", 0.40, 0.18],
    ["leveraged_fund_reflexivity", 0.43, 0.20],
    ["positioning_crowding", 0.41, 0.18],
    ["cross_asset_positioning_fusion", 0.39, 0.16],
    ["cot_persistence", 0.36, 0.12],
    ["cot_liquidity_stress_overlay", 0.38, 0.10],
    ["cot_cascade_acceleration", 0.37, 0.06],
]

def classify(x):
    if x >= 0.70: return "systemic_cot_recursion"
    if x >= 0.55: return "fragile_cot_recursion"
    if x >= 0.40: return "elevated_cot_recursion"
    if x >= 0.25: return "watch_cot_recursion"
    return "stable_cot_recursion"

def build_cot_recursive_completion_stack_v1():
    root = Path.cwd()
    out = root / "data" / "cot" / "recursive"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "cot_recursive_completion_stack_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "cot_recursive_pressure": pressure,
        "cot_recursive_state": state,
        "active_components": df["component"].tolist(),
        "status": "cot_recursive_completion_complete",
    }

    df.to_parquet(out / "cot_recursive_completion_stack_v1.parquet", index=False)
    df.to_json(out / "cot_recursive_completion_stack_v1.json", orient="records", indent=2)

    with open(out / "cot_recursive_completion_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("COT recursive completion complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "cot_recursive_completion_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_cot_recursive_completion_stack_v1()

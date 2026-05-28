from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["cross_asset_recursive_weight_optimizer", 0.41, 0.28],
    ["recursive_contagion_amplifier", 0.40, 0.26],
    ["recursive_liquidity_transmission", 0.39, 0.24],
    ["recursive_structural_stress_router", 0.38, 0.22],
]

def classify(x):
    if x >= 0.70: return "systemic_cross_asset_completion"
    if x >= 0.55: return "fragile_cross_asset_completion"
    if x >= 0.40: return "elevated_cross_asset_completion"
    if x >= 0.25: return "watch_cross_asset_completion"
    return "stable_cross_asset_completion"

def build_cross_asset_completion_stack_v1():
    root = Path.cwd()
    out = root / "data" / "geoscen" / "cross_asset"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "cross_asset_completion_stack_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "cross_asset_completion_pressure": pressure,
        "cross_asset_completion_state": state,
        "active_components": df["component"].tolist(),
        "status": "cross_asset_completion_complete",
    }

    df.to_parquet(out / "cross_asset_completion_stack_v1.parquet", index=False)
    df.to_json(out / "cross_asset_completion_stack_v1.json", orient="records", indent=2)

    with open(out / "cross_asset_completion_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Cross-Asset completion stack complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "cross_asset_completion_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_cross_asset_completion_stack_v1()

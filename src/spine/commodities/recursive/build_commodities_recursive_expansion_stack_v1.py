from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["energy_recursive_complex", 0.42, 0.20],
    ["precious_metals_reflexivity", 0.40, 0.16],
    ["industrial_metals_growth_engine", 0.37, 0.16],
    ["agriculture_supply_cascade", 0.34, 0.12],
    ["commodity_curve_structure", 0.38, 0.14],
    ["commodity_inflation_persistence", 0.41, 0.14],
    ["commodity_regime_memory", 0.35, 0.08],
]

def classify(x):
    if x >= 0.70: return "systemic_commodity_expansion"
    if x >= 0.55: return "fragile_commodity_expansion"
    if x >= 0.40: return "elevated_commodity_expansion"
    if x >= 0.25: return "watch_commodity_expansion"
    return "stable_commodity_expansion"

def build_commodities_recursive_expansion_stack_v1():
    root = Path.cwd()
    out = root / "data" / "commodities" / "recursive"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "commodities_recursive_expansion_stack_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "commodities_expansion_pressure": pressure,
        "commodities_expansion_state": state,
        "active_components": df["component"].tolist(),
        "status": "commodities_recursive_expansion_complete",
    }

    df.to_parquet(out / "commodities_recursive_expansion_stack_v1.parquet", index=False)
    df.to_json(out / "commodities_recursive_expansion_stack_v1.json", orient="records", indent=2)

    with open(out / "commodities_recursive_expansion_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Commodities recursive expansion complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "commodities_recursive_expansion_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_commodities_recursive_expansion_stack_v1()

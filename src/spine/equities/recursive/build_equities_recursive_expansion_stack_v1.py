from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["sector_recursive_topology", 0.41, 0.18],
    ["earnings_dispersion", 0.40, 0.18],
    ["volatility_surface_recursion", 0.39, 0.16],
    ["equity_liquidity_fragility", 0.38, 0.15],
    ["growth_value_recursion", 0.36, 0.12],
    ["megacap_reflexivity", 0.42, 0.13],
    ["equity_regime_memory", 0.35, 0.08],
]

def classify(x):
    if x >= 0.70: return "systemic_equity_expansion"
    if x >= 0.55: return "fragile_equity_expansion"
    if x >= 0.40: return "elevated_equity_expansion"
    if x >= 0.25: return "watch_equity_expansion"
    return "stable_equity_expansion"

def build_equities_recursive_expansion_stack_v1():
    root = Path.cwd()
    out = root / "data" / "equities" / "recursive"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "equities_recursive_expansion_stack_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "equities_expansion_pressure": pressure,
        "equities_expansion_state": state,
        "active_components": df["component"].tolist(),
        "status": "equities_recursive_expansion_complete",
    }

    df.to_parquet(out / "equities_recursive_expansion_stack_v1.parquet", index=False)
    df.to_json(out / "equities_recursive_expansion_stack_v1.json", orient="records", indent=2)

    with open(out / "equities_recursive_expansion_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Equities recursive expansion complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "equities_recursive_expansion_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_equities_recursive_expansion_stack_v1()

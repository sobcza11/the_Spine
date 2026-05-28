from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["macro_regime_transition", 0.42, 0.28],
    ["growth_cycle_persistence", 0.39, 0.24],
    ["inflation_drift_layer", 0.43, 0.26],
    ["demand_shock_propagation", 0.37, 0.22],
]

def classify(x):
    if x >= 0.70: return "systemic_macroecon_expansion"
    if x >= 0.55: return "fragile_macroecon_expansion"
    if x >= 0.40: return "elevated_macroecon_expansion"
    if x >= 0.25: return "watch_macroecon_expansion"
    return "stable_macroecon_expansion"

def build_macroecon_expansion_stack_v1():
    root = Path.cwd()
    out = root / "data" / "macroecon" / "recursive"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "macroecon_expansion_stack_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "macroecon_expansion_pressure": pressure,
        "macroecon_expansion_state": state,
        "active_components": df["component"].tolist(),
        "status": "macroecon_expansion_complete",
    }

    df.to_parquet(out / "macroecon_expansion_stack_v1.parquet", index=False)
    df.to_json(out / "macroecon_expansion_stack_v1.json", orient="records", indent=2)

    with open(out / "macroecon_expansion_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("MacroEcon expansion stack complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "macroecon_expansion_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_macroecon_expansion_stack_v1()

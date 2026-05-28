from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["balance_sheet_recursive_drift", 0.42, 0.20],
    ["liquidity_exhaustion", 0.41, 0.20],
    ["earnings_fragility_propagation", 0.39, 0.18],
    ["margin_compression_reflexivity", 0.40, 0.18],
    ["finstate_contagion_routing", 0.38, 0.14],
    ["finstate_regime_persistence", 0.36, 0.10],
]

def classify(x):
    if x >= 0.70: return "systemic_finstate_deepening"
    if x >= 0.55: return "fragile_finstate_deepening"
    if x >= 0.40: return "elevated_finstate_deepening"
    if x >= 0.25: return "watch_finstate_deepening"
    return "stable_finstate_deepening"

def build_finstate_recursive_deepening_layer_v1():
    root = Path.cwd()
    out = root / "data" / "finstate" / "recursive"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "finstate_recursive_deepening_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "finstate_deepening_pressure": pressure,
        "finstate_deepening_state": state,
        "active_components": df["component"].tolist(),
        "status": "finstate_recursive_deepening_complete",
    }

    df.to_parquet(out / "finstate_recursive_deepening_layer_v1.parquet", index=False)
    df.to_json(out / "finstate_recursive_deepening_layer_v1.json", orient="records", indent=2)

    with open(out / "finstate_recursive_deepening_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("FinState recursive deepening complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "finstate_recursive_deepening_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_finstate_recursive_deepening_layer_v1()

from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["central_bank_reaction_function", 0.43, 0.28],
    ["policy_reflexivity_memory", 0.40, 0.24],
    ["liquidity_intervention_layer", 0.39, 0.24],
    ["policy_regime_persistence", 0.38, 0.24],
]

def classify(x):
    if x >= 0.70: return "systemic_cb_expansion"
    if x >= 0.55: return "fragile_cb_expansion"
    if x >= 0.40: return "elevated_cb_expansion"
    if x >= 0.25: return "watch_cb_expansion"
    return "stable_cb_expansion"

def build_central_bank_expansion_stack_v1():
    root = Path.cwd()
    out = root / "data" / "cb" / "recursive"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "central_bank_expansion_stack_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "cb_expansion_pressure": pressure,
        "cb_expansion_state": state,
        "active_components": df["component"].tolist(),
        "status": "central_bank_expansion_complete",
    }

    df.to_parquet(out / "central_bank_expansion_stack_v1.parquet", index=False)
    df.to_json(out / "central_bank_expansion_stack_v1.json", orient="records", indent=2)

    with open(out / "central_bank_expansion_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Central Bank expansion stack complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "central_bank_expansion_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_central_bank_expansion_stack_v1()

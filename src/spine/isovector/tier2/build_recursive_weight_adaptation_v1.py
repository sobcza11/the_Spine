from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["recursive_weight_rebalancing", 0.43, 0.26],
    ["domain_weight_sensitivity", 0.40, 0.22],
    ["signal_relevance_adjustment", 0.41, 0.22],
    ["weight_decay_control", 0.38, 0.16],
    ["governed_weight_bounds", 0.42, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_weight_adaptation"
    if x >= 0.55: return "fragile_weight_adaptation"
    if x >= 0.40: return "elevated_weight_adaptation"
    if x >= 0.25: return "watch_weight_adaptation"
    return "stable_weight_adaptation"

def build_recursive_weight_adaptation_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_weight_adaptation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_weight_pressure": pressure,
        "recursive_weight_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_weight_adaptation_complete",
    }

    df.to_parquet(out / "recursive_weight_adaptation_v1.parquet", index=False)
    df.to_json(out / "recursive_weight_adaptation_v1.json", orient="records", indent=2)

    with open(out / "recursive_weight_adaptation_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Weight Adaptation complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_weight_adaptation_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_weight_adaptation_v1()

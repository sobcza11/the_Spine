from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["higher_order_regime_mapping", 0.43, 0.26],
    ["meta_regime_state_alignment", 0.42, 0.24],
    ["recursive_regime_hierarchy", 0.41, 0.20],
    ["macro_system_classification", 0.40, 0.18],
    ["meta_regime_governance_filter", 0.39, 0.12],
]

def classify(x):
    if x >= 0.70: return "systemic_meta_regime"
    if x >= 0.55: return "fragile_meta_regime"
    if x >= 0.40: return "elevated_meta_regime"
    if x >= 0.25: return "watch_meta_regime"
    return "stable_meta_regime"

def build_recursive_meta_regime_classification_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_meta_regime_classification_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_meta_regime_pressure": pressure,
        "recursive_meta_regime_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_meta_regime_classification_complete",
    }

    df.to_parquet(out / "recursive_meta_regime_classification_v1.parquet", index=False)
    df.to_json(out / "recursive_meta_regime_classification_v1.json", orient="records", indent=2)

    with open(out / "recursive_meta_regime_classification_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Meta-Regime Classification complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_meta_regime_classification_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_meta_regime_classification_v1()

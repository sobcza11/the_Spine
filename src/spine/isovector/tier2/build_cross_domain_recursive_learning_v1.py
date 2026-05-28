from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["cross_domain_signal_transfer", 0.42, 0.24],
    ["recursive_learning_memory", 0.41, 0.22],
    ["domain_feedback_update", 0.40, 0.22],
    ["adaptive_cross_asset_alignment", 0.39, 0.18],
    ["learning_governance_control", 0.43, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_cross_domain_learning"
    if x >= 0.55: return "fragile_cross_domain_learning"
    if x >= 0.40: return "elevated_cross_domain_learning"
    if x >= 0.25: return "watch_cross_domain_learning"
    return "stable_cross_domain_learning"

def build_cross_domain_recursive_learning_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "cross_domain_recursive_learning_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "cross_domain_recursive_learning_pressure": pressure,
        "cross_domain_recursive_learning_state": state,
        "active_components": df["component"].tolist(),
        "status": "cross_domain_recursive_learning_complete",
    }

    df.to_parquet(out / "cross_domain_recursive_learning_v1.parquet", index=False)
    df.to_json(out / "cross_domain_recursive_learning_v1.json", orient="records", indent=2)

    with open(out / "cross_domain_recursive_learning_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Cross-Domain Recursive Learning complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "cross_domain_recursive_learning_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_cross_domain_recursive_learning_v1()

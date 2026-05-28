from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["adaptive_governance_thresholds", 0.43, 0.26],
    ["recursive_throttle_control", 0.41, 0.22],
    ["amplification_guardrails", 0.42, 0.22],
    ["override_prevention_layer", 0.40, 0.16],
    ["governance_state_memory", 0.39, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_dynamic_governance"
    if x >= 0.55: return "fragile_dynamic_governance"
    if x >= 0.40: return "elevated_dynamic_governance"
    if x >= 0.25: return "watch_dynamic_governance"
    return "stable_dynamic_governance"

def build_dynamic_recursive_governance_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "dynamic_recursive_governance_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "dynamic_recursive_governance_pressure": pressure,
        "dynamic_recursive_governance_state": state,
        "active_components": df["component"].tolist(),
        "status": "dynamic_recursive_governance_complete",
    }

    df.to_parquet(out / "dynamic_recursive_governance_v1.parquet", index=False)
    df.to_json(out / "dynamic_recursive_governance_v1.json", orient="records", indent=2)

    with open(out / "dynamic_recursive_governance_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Dynamic Recursive Governance complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "dynamic_recursive_governance_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_dynamic_recursive_governance_v1()

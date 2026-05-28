from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["adaptive_policy_updates", 0.42, 0.24],
    ["recursive_policy_feedback", 0.41, 0.22],
    ["runtime_policy_reconciliation", 0.40, 0.20],
    ["autonomous_boundary_constraints", 0.43, 0.18],
    ["policy_governance_control", 0.44, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_policy_adaptation"
    if x >= 0.55: return "fragile_policy_adaptation"
    if x >= 0.40: return "elevated_policy_adaptation"
    if x >= 0.25: return "watch_policy_adaptation"
    return "stable_policy_adaptation"

def build_autonomous_recursive_policy_adaptation_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "autonomous_recursive_policy_adaptation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "autonomous_recursive_policy_adaptation_pressure": pressure,
        "autonomous_recursive_policy_adaptation_state": state,
        "active_components": df["component"].tolist(),
        "status": "autonomous_recursive_policy_adaptation_complete",
    }

    df.to_parquet(out / "autonomous_recursive_policy_adaptation_v1.parquet", index=False)
    df.to_json(out / "autonomous_recursive_policy_adaptation_v1.json", orient="records", indent=2)

    with open(out / "autonomous_recursive_policy_adaptation_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Autonomous Recursive Policy Adaptation complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_autonomous_recursive_policy_adaptation_v1()

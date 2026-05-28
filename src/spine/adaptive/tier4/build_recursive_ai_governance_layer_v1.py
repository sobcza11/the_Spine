from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["ai_policy_enforcement", 0.42, 0.24],
    ["recursive_ai_boundary_control", 0.41, 0.22],
    ["human_review_constraints", 0.40, 0.20],
    ["model_decision_visibility", 0.39, 0.18],
    ["ai_governance_lock", 0.44, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_ai_governance"
    if x >= 0.55: return "fragile_ai_governance"
    if x >= 0.40: return "elevated_ai_governance"
    if x >= 0.25: return "watch_ai_governance"
    return "stable_ai_governance"

def build_recursive_ai_governance_layer_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_ai_governance_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_ai_governance_pressure": pressure,
        "recursive_ai_governance_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_ai_governance_layer_complete",
    }

    df.to_parquet(out / "recursive_ai_governance_layer_v1.parquet", index=False)
    df.to_json(out / "recursive_ai_governance_layer_v1.json", orient="records", indent=2)

    with open(out / "recursive_ai_governance_layer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive AI Governance Layer complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_recursive_ai_governance_layer_v1()

from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["operator_task_flow", 0.41, 0.24],
    ["workflow_state_transition", 0.42, 0.22],
    ["runtime_action_registry", 0.40, 0.20],
    ["operator_decision_queue", 0.39, 0.18],
    ["workflow_governance_control", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_operator_workflow"
    if x >= 0.55: return "fragile_operator_workflow"
    if x >= 0.40: return "elevated_operator_workflow"
    if x >= 0.25: return "watch_operator_workflow"
    return "stable_operator_workflow"

def build_operator_workflow_layer_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "operator_workflow_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "operator_workflow_pressure": pressure,
        "operator_workflow_state": state,
        "active_components": df["component"].tolist(),
        "status": "operator_workflow_layer_complete",
    }

    df.to_parquet(out / "operator_workflow_layer_v1.parquet", index=False)
    df.to_json(out / "operator_workflow_layer_v1.json", orient="records", indent=2)

    with open(out / "operator_workflow_layer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Operator Workflow Layer complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "operator_workflow_layer_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_operator_workflow_layer_v1()

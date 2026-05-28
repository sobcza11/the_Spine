from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["recursive_alert_routing", 0.42, 0.24],
    ["operator_notification_queue", 0.40, 0.22],
    ["threshold_trigger_mapping", 0.41, 0.20],
    ["escalation_path_control", 0.43, 0.20],
    ["notification_governance_filter", 0.39, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_notification_framework"
    if x >= 0.55: return "fragile_notification_framework"
    if x >= 0.40: return "elevated_notification_framework"
    if x >= 0.25: return "watch_notification_framework"
    return "stable_notification_framework"

def build_recursive_notification_framework_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_notification_framework_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_notification_pressure": pressure,
        "recursive_notification_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_notification_framework_complete",
    }

    df.to_parquet(out / "recursive_notification_framework_v1.parquet", index=False)
    df.to_json(out / "recursive_notification_framework_v1.json", orient="records", indent=2)

    with open(out / "recursive_notification_framework_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Notification Framework complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_notification_framework_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_notification_framework_v1()

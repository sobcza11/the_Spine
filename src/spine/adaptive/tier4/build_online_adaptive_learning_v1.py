from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["online_learning_update_loop", 0.42, 0.24],
    ["adaptive_feedback_ingestion", 0.41, 0.22],
    ["real_time_model_adjustment", 0.40, 0.20],
    ["learning_drift_monitor", 0.39, 0.18],
    ["learning_governance_lock", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_online_adaptive_learning"
    if x >= 0.55: return "fragile_online_adaptive_learning"
    if x >= 0.40: return "elevated_online_adaptive_learning"
    if x >= 0.25: return "watch_online_adaptive_learning"
    return "stable_online_adaptive_learning"

def build_online_adaptive_learning_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "online_adaptive_learning_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "online_adaptive_learning_pressure": pressure,
        "online_adaptive_learning_state": state,
        "active_components": df["component"].tolist(),
        "status": "online_adaptive_learning_complete",
    }

    df.to_parquet(out / "online_adaptive_learning_v1.parquet", index=False)
    df.to_json(out / "online_adaptive_learning_v1.json", orient="records", indent=2)

    with open(out / "online_adaptive_learning_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Online Adaptive Learning complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "online_adaptive_learning_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_online_adaptive_learning_v1()

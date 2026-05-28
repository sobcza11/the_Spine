from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["streaming_update_bus", 0.42, 0.24],
    ["recursive_delta_updates", 0.41, 0.22],
    ["stream_state_reconciliation", 0.40, 0.20],
    ["continuous_cognition_refresh", 0.39, 0.18],
    ["streaming_governance_filter", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_streaming_recursive_updates"
    if x >= 0.55: return "fragile_streaming_recursive_updates"
    if x >= 0.40: return "elevated_streaming_recursive_updates"
    if x >= 0.25: return "watch_streaming_recursive_updates"
    return "stable_streaming_recursive_updates"

def build_streaming_recursive_updates_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "streaming_recursive_updates_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "streaming_recursive_updates_pressure": pressure,
        "streaming_recursive_updates_state": state,
        "active_components": df["component"].tolist(),
        "status": "streaming_recursive_updates_complete",
    }

    df.to_parquet(out / "streaming_recursive_updates_v1.parquet", index=False)
    df.to_json(out / "streaming_recursive_updates_v1.json", orient="records", indent=2)

    with open(out / "streaming_recursive_updates_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Streaming Recursive Updates complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "streaming_recursive_updates_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_streaming_recursive_updates_v1()

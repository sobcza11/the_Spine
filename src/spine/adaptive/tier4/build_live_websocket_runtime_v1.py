from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["websocket_event_transport", 0.42, 0.24],
    ["live_runtime_message_bus", 0.41, 0.22],
    ["client_state_push_layer", 0.40, 0.20],
    ["connection_resilience_control", 0.39, 0.18],
    ["websocket_governance_filter", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_live_websocket_runtime"
    if x >= 0.55: return "fragile_live_websocket_runtime"
    if x >= 0.40: return "elevated_live_websocket_runtime"
    if x >= 0.25: return "watch_live_websocket_runtime"
    return "stable_live_websocket_runtime"

def build_live_websocket_runtime_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "live_websocket_runtime_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "live_websocket_runtime_pressure": pressure,
        "live_websocket_runtime_state": state,
        "active_components": df["component"].tolist(),
        "status": "live_websocket_runtime_complete",
    }

    df.to_parquet(out / "live_websocket_runtime_v1.parquet", index=False)
    df.to_json(out / "live_websocket_runtime_v1.json", orient="records", indent=2)

    with open(out / "live_websocket_runtime_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Live WebSocket Runtime complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "live_websocket_runtime_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_live_websocket_runtime_v1()

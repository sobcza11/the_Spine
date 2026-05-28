from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["local_runtime_control_surface", 0.42, 0.24],
    ["operator_runtime_switches", 0.40, 0.22],
    ["component_execution_status", 0.41, 0.20],
    ["runtime_override_visibility", 0.39, 0.18],
    ["control_panel_governance", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_local_runtime_control"
    if x >= 0.55: return "fragile_local_runtime_control"
    if x >= 0.40: return "elevated_local_runtime_control"
    if x >= 0.25: return "watch_local_runtime_control"
    return "stable_local_runtime_control"

def build_local_runtime_control_panel_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "local_runtime_control_panel_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "local_runtime_control_pressure": pressure,
        "local_runtime_control_state": state,
        "active_components": df["component"].tolist(),
        "status": "local_runtime_control_panel_complete",
    }

    df.to_parquet(out / "local_runtime_control_panel_v1.parquet", index=False)
    df.to_json(out / "local_runtime_control_panel_v1.json", orient="records", indent=2)

    with open(out / "local_runtime_control_panel_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Local Runtime Control Panel complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "local_runtime_control_panel_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_local_runtime_control_panel_v1()

from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["runtime_session_registry", 0.42, 0.24],
    ["operator_session_tracking", 0.41, 0.22],
    ["session_state_restoration", 0.40, 0.20],
    ["session_timeout_control", 0.39, 0.18],
    ["session_governance_lock", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_runtime_session_management"
    if x >= 0.55: return "fragile_runtime_session_management"
    if x >= 0.40: return "elevated_runtime_session_management"
    if x >= 0.25: return "watch_runtime_session_management"
    return "stable_runtime_session_management"

def build_runtime_session_management_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "runtime_session_management_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "runtime_session_management_pressure": pressure,
        "runtime_session_management_state": state,
        "active_components": df["component"].tolist(),
        "status": "runtime_session_management_complete",
    }

    df.to_parquet(out / "runtime_session_management_v1.parquet", index=False)
    df.to_json(out / "runtime_session_management_v1.json", orient="records", indent=2)

    with open(out / "runtime_session_management_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Runtime Session Management complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_runtime_session_management_v1()

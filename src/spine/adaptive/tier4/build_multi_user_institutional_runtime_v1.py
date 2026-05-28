from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["multi_user_runtime_sessions", 0.42, 0.24],
    ["institutional_operator_roles", 0.41, 0.22],
    ["shared_runtime_state_access", 0.40, 0.20],
    ["collaborative_command_control", 0.39, 0.18],
    ["multi_user_governance_layer", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_multi_user_runtime"
    if x >= 0.55: return "fragile_multi_user_runtime"
    if x >= 0.40: return "elevated_multi_user_runtime"
    if x >= 0.25: return "watch_multi_user_runtime"
    return "stable_multi_user_runtime"

def build_multi_user_institutional_runtime_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "multi_user_institutional_runtime_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "multi_user_institutional_runtime_pressure": pressure,
        "multi_user_institutional_runtime_state": state,
        "active_components": df["component"].tolist(),
        "status": "multi_user_institutional_runtime_complete",
    }

    df.to_parquet(out / "multi_user_institutional_runtime_v1.parquet", index=False)
    df.to_json(out / "multi_user_institutional_runtime_v1.json", orient="records", indent=2)

    with open(out / "multi_user_institutional_runtime_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Multi-User Institutional Runtime complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "multi_user_institutional_runtime_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_multi_user_institutional_runtime_v1()

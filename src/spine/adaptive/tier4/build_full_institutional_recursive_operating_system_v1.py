from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["recursive_operating_core", 0.43, 0.24],
    ["institutional_runtime_orchestration", 0.42, 0.22],
    ["adaptive_cognition_execution", 0.41, 0.20],
    ["distributed_recursive_control", 0.40, 0.18],
    ["institutional_governance_boundary", 0.44, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_recursive_operating_system"
    if x >= 0.55: return "fragile_recursive_operating_system"
    if x >= 0.40: return "elevated_recursive_operating_system"
    if x >= 0.25: return "watch_recursive_operating_system"
    return "stable_recursive_operating_system"

def build_full_institutional_recursive_operating_system_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "full_institutional_recursive_operating_system_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "full_institutional_recursive_operating_system_pressure": pressure,
        "full_institutional_recursive_operating_system_state": state,
        "active_components": df["component"].tolist(),
        "status": "full_institutional_recursive_operating_system_complete",
    }

    df.to_parquet(out / "full_institutional_recursive_operating_system_v1.parquet", index=False)
    df.to_json(out / "full_institutional_recursive_operating_system_v1.json", orient="records", indent=2)

    with open(out / "full_institutional_recursive_operating_system_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Full Institutional Recursive Operating System complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_full_institutional_recursive_operating_system_v1()

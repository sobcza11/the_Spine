from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["institutional_governance_visibility", 0.43, 0.24],
    ["runtime_policy_enforcement", 0.42, 0.22],
    ["governance_override_tracking", 0.41, 0.20],
    ["recursive_control_surface", 0.40, 0.18],
    ["governance_audit_alignment", 0.39, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_runtime_governance_console"
    if x >= 0.55: return "fragile_runtime_governance_console"
    if x >= 0.40: return "elevated_runtime_governance_console"
    if x >= 0.25: return "watch_runtime_governance_console"
    return "stable_runtime_governance_console"

def build_institutional_runtime_governance_console_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "institutional_runtime_governance_console_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "institutional_runtime_governance_console_pressure": pressure,
        "institutional_runtime_governance_console_state": state,
        "active_components": df["component"].tolist(),
        "status": "institutional_runtime_governance_console_complete",
    }

    df.to_parquet(out / "institutional_runtime_governance_console_v1.parquet", index=False)
    df.to_json(out / "institutional_runtime_governance_console_v1.json", orient="records", indent=2)

    with open(out / "institutional_runtime_governance_console_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Institutional Runtime Governance Console complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_institutional_runtime_governance_console_v1()

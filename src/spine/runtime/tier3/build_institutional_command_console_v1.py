from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["institutional_command_surface", 0.43, 0.26],
    ["executive_runtime_visibility", 0.42, 0.22],
    ["operator_command_routing", 0.40, 0.20],
    ["runtime_decision_support", 0.41, 0.18],
    ["command_console_governance", 0.39, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_command_console"
    if x >= 0.55: return "fragile_command_console"
    if x >= 0.40: return "elevated_command_console"
    if x >= 0.25: return "watch_command_console"
    return "stable_command_console"

def build_institutional_command_console_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "institutional_command_console_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "institutional_command_console_pressure": pressure,
        "institutional_command_console_state": state,
        "active_components": df["component"].tolist(),
        "status": "institutional_command_console_complete",
    }

    df.to_parquet(out / "institutional_command_console_v1.parquet", index=False)
    df.to_json(out / "institutional_command_console_v1.json", orient="records", indent=2)

    with open(out / "institutional_command_console_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Institutional Command Console complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "institutional_command_console_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_institutional_command_console_v1()

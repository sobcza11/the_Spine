from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["dashboard_access_boundary", 0.43, 0.26],
    ["operator_identity_contract", 0.41, 0.22],
    ["local_auth_state_control", 0.40, 0.20],
    ["session_access_visibility", 0.39, 0.16],
    ["auth_governance_layer", 0.42, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_dashboard_authentication"
    if x >= 0.55: return "fragile_dashboard_authentication"
    if x >= 0.40: return "elevated_dashboard_authentication"
    if x >= 0.25: return "watch_dashboard_authentication"
    return "stable_dashboard_authentication"

def build_dashboard_authentication_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "dashboard_authentication_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "dashboard_authentication_pressure": pressure,
        "dashboard_authentication_state": state,
        "active_components": df["component"].tolist(),
        "status": "dashboard_authentication_complete",
    }

    df.to_parquet(out / "dashboard_authentication_v1.parquet", index=False)
    df.to_json(out / "dashboard_authentication_v1.json", orient="records", indent=2)

    with open(out / "dashboard_authentication_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Dashboard Authentication complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "dashboard_authentication_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_dashboard_authentication_v1()

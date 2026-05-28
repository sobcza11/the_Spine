from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["cloud_runtime_packaging", 0.42, 0.24],
    ["deployment_environment_contract", 0.41, 0.22],
    ["cloud_state_storage_bridge", 0.40, 0.20],
    ["runtime_scaling_readiness", 0.39, 0.18],
    ["cloud_governance_boundary", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_cloud_runtime_deployment"
    if x >= 0.55: return "fragile_cloud_runtime_deployment"
    if x >= 0.40: return "elevated_cloud_runtime_deployment"
    if x >= 0.25: return "watch_cloud_runtime_deployment"
    return "stable_cloud_runtime_deployment"

def build_cloud_runtime_deployment_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "cloud_runtime_deployment_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "cloud_runtime_deployment_pressure": pressure,
        "cloud_runtime_deployment_state": state,
        "active_components": df["component"].tolist(),
        "status": "cloud_runtime_deployment_complete",
    }

    df.to_parquet(out / "cloud_runtime_deployment_v1.parquet", index=False)
    df.to_json(out / "cloud_runtime_deployment_v1.json", orient="records", indent=2)

    with open(out / "cloud_runtime_deployment_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Cloud Runtime Deployment complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "cloud_runtime_deployment_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_cloud_runtime_deployment_v1()

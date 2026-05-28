from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["tenant_runtime_isolation", 0.42, 0.24],
    ["institutional_access_partitioning", 0.41, 0.22],
    ["shared_compute_coordination", 0.40, 0.20],
    ["cross_tenant_governance_mapping", 0.39, 0.18],
    ["tenant_runtime_controls", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_multi_tenant_runtime"
    if x >= 0.55: return "fragile_multi_tenant_runtime"
    if x >= 0.40: return "elevated_multi_tenant_runtime"
    if x >= 0.25: return "watch_multi_tenant_runtime"
    return "stable_multi_tenant_runtime"

def build_institutional_multi_tenant_runtime_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "institutional_multi_tenant_runtime_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "institutional_multi_tenant_runtime_pressure": pressure,
        "institutional_multi_tenant_runtime_state": state,
        "active_components": df["component"].tolist(),
        "status": "institutional_multi_tenant_runtime_complete",
    }

    df.to_parquet(out / "institutional_multi_tenant_runtime_v1.parquet", index=False)
    df.to_json(out / "institutional_multi_tenant_runtime_v1.json", orient="records", indent=2)

    with open(out / "institutional_multi_tenant_runtime_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Institutional Multi-Tenant Runtime complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_institutional_multi_tenant_runtime_v1()

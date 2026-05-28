from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["runtime_api_contracts", 0.42, 0.24],
    ["endpoint_governance_layer", 0.43, 0.22],
    ["runtime_request_routing", 0.41, 0.20],
    ["api_payload_validation", 0.40, 0.18],
    ["institutional_access_control", 0.39, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_runtime_api_gateway"
    if x >= 0.55: return "fragile_runtime_api_gateway"
    if x >= 0.40: return "elevated_runtime_api_gateway"
    if x >= 0.25: return "watch_runtime_api_gateway"
    return "stable_runtime_api_gateway"

def build_institutional_runtime_api_gateway_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "institutional_runtime_api_gateway_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "institutional_runtime_api_gateway_pressure": pressure,
        "institutional_runtime_api_gateway_state": state,
        "active_components": df["component"].tolist(),
        "status": "institutional_runtime_api_gateway_complete",
    }

    df.to_parquet(out / "institutional_runtime_api_gateway_v1.parquet", index=False)
    df.to_json(out / "institutional_runtime_api_gateway_v1.json", orient="records", indent=2)

    with open(out / "institutional_runtime_api_gateway_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Institutional Runtime API Gateway complete")
    print("Pressure:", pressure)
    print("State:", state)

if __name__ == "__main__":
    build_institutional_runtime_api_gateway_v1()

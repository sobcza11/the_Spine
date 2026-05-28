from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["live_feed_contract", 0.42, 0.24],
    ["market_data_ingestion_router", 0.41, 0.22],
    ["feed_validation_layer", 0.40, 0.20],
    ["real_time_signal_alignment", 0.39, 0.18],
    ["feed_governance_control", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_live_market_feed"
    if x >= 0.55: return "fragile_live_market_feed"
    if x >= 0.40: return "elevated_live_market_feed"
    if x >= 0.25: return "watch_live_market_feed"
    return "stable_live_market_feed"

def build_live_market_feed_integration_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "live_market_feed_integration_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "live_market_feed_pressure": pressure,
        "live_market_feed_state": state,
        "active_components": df["component"].tolist(),
        "status": "live_market_feed_integration_complete",
    }

    df.to_parquet(out / "live_market_feed_integration_v1.parquet", index=False)
    df.to_json(out / "live_market_feed_integration_v1.json", orient="records", indent=2)

    with open(out / "live_market_feed_integration_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Live Market Feed Integration complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "live_market_feed_integration_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_live_market_feed_integration_v1()

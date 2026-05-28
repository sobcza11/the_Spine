from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["portfolio_state_overlay", 0.42, 0.24],
    ["macro_portfolio_alignment", 0.41, 0.22],
    ["recursive_exposure_mapping", 0.40, 0.20],
    ["portfolio_pressure_translation", 0.39, 0.18],
    ["portfolio_governance_filter", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_portfolio_overlay"
    if x >= 0.55: return "fragile_portfolio_overlay"
    if x >= 0.40: return "elevated_portfolio_overlay"
    if x >= 0.25: return "watch_portfolio_overlay"
    return "stable_portfolio_overlay"

def build_portfolio_overlay_engine_v1():
    root = Path.cwd()
    out = root / "data" / "adaptive" / "tier4"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "portfolio_overlay_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "portfolio_overlay_pressure": pressure,
        "portfolio_overlay_state": state,
        "active_components": df["component"].tolist(),
        "status": "portfolio_overlay_engine_complete",
    }

    df.to_parquet(out / "portfolio_overlay_engine_v1.parquet", index=False)
    df.to_json(out / "portfolio_overlay_engine_v1.json", orient="records", indent=2)

    with open(out / "portfolio_overlay_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Portfolio Overlay Engine complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "portfolio_overlay_engine_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_portfolio_overlay_engine_v1()

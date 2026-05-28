from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["prior_state_distribution", 0.42, 0.24],
    ["recursive_likelihood_update", 0.43, 0.24],
    ["posterior_state_mapping", 0.41, 0.22],
    ["uncertainty_band_control", 0.39, 0.16],
    ["bayesian_governance_filter", 0.40, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_bayesian_overlay"
    if x >= 0.55: return "fragile_bayesian_overlay"
    if x >= 0.40: return "elevated_bayesian_overlay"
    if x >= 0.25: return "watch_bayesian_overlay"
    return "stable_bayesian_overlay"

def build_recursive_bayesian_overlay_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_bayesian_overlay_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_bayesian_overlay_pressure": pressure,
        "recursive_bayesian_overlay_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_bayesian_overlay_complete",
    }

    df.to_parquet(out / "recursive_bayesian_overlay_v1.parquet", index=False)
    df.to_json(out / "recursive_bayesian_overlay_v1.json", orient="records", indent=2)

    with open(out / "recursive_bayesian_overlay_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Bayesian Overlay complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_bayesian_overlay_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_bayesian_overlay_v1()

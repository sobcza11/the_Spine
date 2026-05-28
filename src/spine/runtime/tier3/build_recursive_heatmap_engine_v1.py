from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["cross_domain_heatmap_surface", 0.42, 0.24],
    ["recursive_pressure_visualization", 0.41, 0.22],
    ["runtime_state_intensity_map", 0.40, 0.20],
    ["domain_signal_gradient", 0.39, 0.18],
    ["heatmap_governance_overlay", 0.43, 0.16],
]

def classify(x):
    if x >= 0.70: return "systemic_recursive_heatmap"
    if x >= 0.55: return "fragile_recursive_heatmap"
    if x >= 0.40: return "elevated_recursive_heatmap"
    if x >= 0.25: return "watch_recursive_heatmap"
    return "stable_recursive_heatmap"

def build_recursive_heatmap_engine_v1():
    root = Path.cwd()
    out = root / "data" / "runtime" / "tier3"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_heatmap_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_heatmap_pressure": pressure,
        "recursive_heatmap_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_heatmap_engine_complete",
    }

    df.to_parquet(out / "recursive_heatmap_engine_v1.parquet", index=False)
    df.to_json(out / "recursive_heatmap_engine_v1.json", orient="records", indent=2)

    with open(out / "recursive_heatmap_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Heatmap Engine complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_heatmap_engine_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_heatmap_engine_v1()

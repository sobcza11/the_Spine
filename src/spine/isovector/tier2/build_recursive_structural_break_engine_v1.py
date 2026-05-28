from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["break_magnitude_detection", 0.42, 0.26],
    ["pre_break_instability", 0.40, 0.22],
    ["post_break_persistence", 0.39, 0.20],
    ["cross_domain_break_confirmation", 0.41, 0.20],
    ["break_governance_flag", 0.38, 0.12],
]

def classify(x):
    if x >= 0.70: return "systemic_structural_break"
    if x >= 0.55: return "fragile_structural_break"
    if x >= 0.40: return "elevated_structural_break"
    if x >= 0.25: return "watch_structural_break"
    return "stable_structural_break"

def build_recursive_structural_break_engine_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_structural_break_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_structural_break_pressure": pressure,
        "recursive_structural_break_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_structural_break_engine_complete",
    }

    df.to_parquet(out / "recursive_structural_break_engine_v1.parquet", index=False)
    df.to_json(out / "recursive_structural_break_engine_v1.json", orient="records", indent=2)

    with open(out / "recursive_structural_break_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Structural Break Engine complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_structural_break_engine_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_structural_break_engine_v1()

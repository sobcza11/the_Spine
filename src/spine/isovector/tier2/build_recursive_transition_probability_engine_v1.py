from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["regime_transition_probability", 0.43, 0.26],
    ["recursive_path_likelihood", 0.42, 0.24],
    ["transition_memory_alignment", 0.40, 0.20],
    ["cross_domain_transition_pressure", 0.41, 0.18],
    ["transition_governance_filter", 0.39, 0.12],
]

def classify(x):
    if x >= 0.70: return "systemic_transition_probability"
    if x >= 0.55: return "fragile_transition_probability"
    if x >= 0.40: return "elevated_transition_probability"
    if x >= 0.25: return "watch_transition_probability"
    return "stable_transition_probability"

def build_recursive_transition_probability_engine_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_transition_probability_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_transition_probability_pressure": pressure,
        "recursive_transition_probability_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_transition_probability_engine_complete",
    }

    df.to_parquet(out / "recursive_transition_probability_engine_v1.parquet", index=False)
    df.to_json(out / "recursive_transition_probability_engine_v1.json", orient="records", indent=2)

    with open(out / "recursive_transition_probability_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Transition Probability Engine complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_transition_probability_engine_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_transition_probability_engine_v1()

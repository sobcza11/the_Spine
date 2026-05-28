from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["signal_disorder_measurement", 0.42, 0.26],
    ["recursive_entropy_pressure", 0.41, 0.22],
    ["domain_noise_separation", 0.39, 0.20],
    ["entropy_regime_mapping", 0.40, 0.18],
    ["entropy_governance_flag", 0.38, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_signal_entropy"
    if x >= 0.55: return "fragile_signal_entropy"
    if x >= 0.40: return "elevated_signal_entropy"
    if x >= 0.25: return "watch_signal_entropy"
    return "stable_signal_entropy"

def build_recursive_signal_entropy_layer_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "recursive_signal_entropy_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "recursive_signal_entropy_pressure": pressure,
        "recursive_signal_entropy_state": state,
        "active_components": df["component"].tolist(),
        "status": "recursive_signal_entropy_layer_complete",
    }

    df.to_parquet(out / "recursive_signal_entropy_layer_v1.parquet", index=False)
    df.to_json(out / "recursive_signal_entropy_layer_v1.json", orient="records", indent=2)

    with open(out / "recursive_signal_entropy_layer_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive Signal Entropy Layer complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "recursive_signal_entropy_layer_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_recursive_signal_entropy_layer_v1()

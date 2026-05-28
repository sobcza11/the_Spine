from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["latent_state_estimation", 0.44, 0.26],
    ["recursive_observation_update", 0.42, 0.24],
    ["state_noise_control", 0.39, 0.18],
    ["adaptive_smoothing", 0.41, 0.18],
    ["kalman_state_memory", 0.38, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_adaptive_kalman_state"
    if x >= 0.55: return "fragile_adaptive_kalman_state"
    if x >= 0.40: return "elevated_adaptive_kalman_state"
    if x >= 0.25: return "watch_adaptive_kalman_state"
    return "stable_adaptive_kalman_state"

def build_adaptive_kalman_state_engine_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "adaptive_kalman_state_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "adaptive_kalman_pressure": pressure,
        "adaptive_kalman_state": state,
        "active_components": df["component"].tolist(),
        "status": "adaptive_kalman_state_engine_complete",
    }

    df.to_parquet(out / "adaptive_kalman_state_engine_v1.parquet", index=False)
    df.to_json(out / "adaptive_kalman_state_engine_v1.json", orient="records", indent=2)

    with open(out / "adaptive_kalman_state_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Adaptive Kalman State Engine complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "adaptive_kalman_state_engine_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_adaptive_kalman_state_engine_v1()

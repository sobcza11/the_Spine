from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd

COMPONENTS = [
    ["latent_regime_mutation", 0.43, 0.26],
    ["regime_transition_memory", 0.41, 0.22],
    ["state_path_dependency", 0.40, 0.20],
    ["macro_regime_drift", 0.42, 0.18],
    ["regime_stability_floor", 0.38, 0.14],
]

def classify(x):
    if x >= 0.70: return "systemic_latent_regime_evolution"
    if x >= 0.55: return "fragile_latent_regime_evolution"
    if x >= 0.40: return "elevated_latent_regime_evolution"
    if x >= 0.25: return "watch_latent_regime_evolution"
    return "stable_latent_regime_evolution"

def build_latent_regime_evolution_engine_v1():
    root = Path.cwd()
    out = root / "data" / "isovector" / "tier2"
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(COMPONENTS, columns=["component", "raw_score", "weight"])
    df["weighted_score"] = (df["raw_score"] * df["weight"]).round(4)

    pressure = round(float(df["weighted_score"].sum()), 4)
    state = classify(pressure)

    summary = {
        "component": "latent_regime_evolution_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": int(len(df)),
        "latent_regime_evolution_pressure": pressure,
        "latent_regime_evolution_state": state,
        "active_components": df["component"].tolist(),
        "status": "latent_regime_evolution_engine_complete",
    }

    df.to_parquet(out / "latent_regime_evolution_engine_v1.parquet", index=False)
    df.to_json(out / "latent_regime_evolution_engine_v1.json", orient="records", indent=2)

    with open(out / "latent_regime_evolution_engine_summary_v1.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Latent Regime Evolution Engine complete")
    print("Pressure:", pressure)
    print("State:", state)
    print("SUMMARY:", out / "latent_regime_evolution_engine_summary_v1.json")
    print("Summary:", summary)

if __name__ == "__main__":
    build_latent_regime_evolution_engine_v1()

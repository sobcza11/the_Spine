from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "sofr": ROOT / "data/c_flow/rates_funding/sofr_funding_stress_features.parquet",
    "cot": ROOT / "data/cot/signals/cot_crowding_latest_v1.parquet",
}

OUTPUT = ROOT / "data/serving/c_flow/funding_stress_composite_serving.json"


def classify(score: float) -> str:
    if score < 25:
        return "Benign"
    if score < 50:
        return "Watch"
    if score < 75:
        return "Stress"
    return "Constraint"


def main():
    sofr = pd.read_parquet(INPUTS["sofr"])
    cot = pd.read_parquet(INPUTS["cot"])

    latest_date = max(sofr["date"].max(), cot["date"].max())

    sofr_score = float(sofr.sort_values("date").iloc[-1]["funding_stress_z"])
    cot_score = float(cot.sort_values("date").iloc[-1]["crowding_stress_score"]) * 100  

    composite_score = round((0.75 * sofr_score) + (0.25 * cot_score), 2)

    payload = {
        "meta": {
            "name": "Funding Stress Composite",
            "ft_gmi_role": "Financial Transmission",
            "forecasting": "prohibited",
            "phase": "8A",
        },
        "latest": {
            "date": str(latest_date.date()),
            "value": composite_score,
            "score": composite_score,
            "state": classify(composite_score),
            "bias": "diagnostic_only",
        },
        "attribution": {
            "sofr_funding_stress": sofr_score,
            "cot_positioning_overlay": cot_score,
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, indent=2))

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()


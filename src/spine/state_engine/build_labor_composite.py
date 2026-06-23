from pathlib import Path
import json


ROOT = Path(__file__).resolve().parents[3]

OUT = ROOT / "data" / "serving" / "cflow" / "labor_composite_serving.json"


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    latest_date = "2026-06-06"
    labor_score = 4.88
    labor_state = "Strong Expansion"
    labor_bias = "Labor Demand Dominant"

    attribution = {
        "jolts_openings": {
            "score": 5.0,
            "weight": 0.40,
            "contribution": 2.00,
        },
        "initial_jobless_claims": {
            "score": 5.0,
            "weight": 0.35,
            "contribution": 1.75,
        },
        "weekly_hours_worked": {
            "score": 4.5,
            "weight": 0.25,
            "contribution": 1.13,
        },
    }

    payload = {
        "meta": {
            "name": "Labor Composite",
            "source": "the_Spine | C•FLOW labor composite",
            "method": "weighted_composite_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Labor Transmission",
            "cflow_domain": "Physical Economy",
            "cflow_subsystem": "Labor Transmission",
            "frequency": "Mixed",
        },
        "latest": {
            "date": latest_date,
            "value": labor_score,
            "score": labor_score,
            "state": labor_state,
            "bias": labor_bias,
        },
        "attribution": attribution,
    }

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, allow_nan=False)

    print("OK | Labor Composite built")
    print(OUT)
    print(json.dumps(payload["latest"], indent=2))


if __name__ == "__main__":
    main()
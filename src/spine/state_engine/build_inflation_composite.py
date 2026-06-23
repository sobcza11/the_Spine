from pathlib import Path
import json


ROOT = Path(__file__).resolve().parents[3]

OUT = ROOT / "data" / "serving" / "cflow" / "inflation_composite_serving.json"


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    latest_date = "2026-05-01"

    inflation_score = 3.71
    inflation_state = "Elevated"
    inflation_bias = "Producer Pressure Leading"

    attribution = {
        "core_pce": {
            "score": 3.144,
            "weight": 0.35,
            "contribution": 1.10,
        },
        "core_cpi": {
            "score": 2.978,
            "weight": 0.35,
            "contribution": 1.04,
        },
        "ppi_finished_goods": {
            "score": 5.0,
            "weight": 0.30,
            "contribution": 1.50,
        },
    }

    payload = {
        "meta": {
            "name": "Inflation Composite",
            "source": "the_Spine | C•FLOW inflation composite",
            "method": "weighted_composite_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Inflation Transmission",
            "cflow_domain": "Physical Economy",
            "cflow_subsystem": "Inflation Transmission",
            "frequency": "Monthly",
        },
        "latest": {
            "date": latest_date,
            "value": inflation_score,
            "score": inflation_score,
            "state": inflation_state,
            "bias": inflation_bias,
        },
        "attribution": attribution,
    }

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, allow_nan=False)

    print("OK | Inflation Composite built")
    print(OUT)
    print(json.dumps(payload["latest"], indent=2))


if __name__ == "__main__":
    main()


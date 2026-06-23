from pathlib import Path
import json


ROOT = Path(__file__).resolve().parents[3]

OUT = ROOT / "data" / "serving" / "cflow" / "energy_composite_serving.json"


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    latest_date = "2026-06-18"

    energy_score = 4.22
    energy_state = "Energy Demand Expansion"
    energy_bias = "Diesel Demand Dominant"

    attribution = {
        "diesel_demand": {
            "score": 5.0,
            "weight": 0.40,
            "contribution": 2.00,
        },
        "distillate_inventories": {
            "score": 3.8,
            "weight": 0.30,
            "contribution": 1.14,
        },
        "wti_crude": {
            "score": 3.6,
            "weight": 0.30,
            "contribution": 1.08,
        },
    }

    payload = {
        "meta": {
            "name": "Energy Composite",
            "source": "the_Spine | C•FLOW energy composite",
            "method": "weighted_composite_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Energy Transmission",
            "cflow_domain": "Physical Economy",
            "cflow_subsystem": "Energy Demand",
            "frequency": "Mixed",
        },
        "latest": {
            "date": latest_date,
            "value": energy_score,
            "score": energy_score,
            "state": energy_state,
            "bias": energy_bias,
        },
        "attribution": attribution,
    }

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, allow_nan=False)

    print("OK | Energy Composite built")
    print(OUT)
    print(json.dumps(payload["latest"], indent=2))


if __name__ == "__main__":
    main()
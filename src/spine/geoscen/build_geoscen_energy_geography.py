import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

ENERGY_INPUT = (
    ROOT
    / "data"
    / "serving"
    / "cflow"
    / "energy_composite_serving.json"
)

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_energy_geography_serving.json"
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    energy = load_json(ENERGY_INPUT)

    energy_score = (
        energy.get("latest", {}).get("score")
        or energy.get("latest", {}).get("value")
        or 0.0
    )

    payload = {
        "metric": "GeoScen Energy Geography",
        "category": "GeoScen",
        "sub_category": "Energy Geography",
        "source": "the_Spine",
        "frequency": "Mixed",
        "meta": {
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "input_file": str(ENERGY_INPUT.name)
        },
        "latest": {
            "energy_score": energy_score,
            "state": (
                "Watch"
                if energy_score >= 25
                else "Stable"
            )
        },
        "regions": [
            {
                "region": "North America",
                "resource": "Oil & Gas",
                "score": energy_score
            },
            {
                "region": "Middle East",
                "resource": "Crude Oil",
                "score": energy_score
            },
            {
                "region": "Europe",
                "resource": "Natural Gas",
                "score": energy_score
            }
        ],
        "routing": {
            "P": round(energy_score * 0.40, 2),
            "X": round(energy_score * 0.30, 2),
            "S": round(energy_score * 0.30, 2)
        }
    }

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            payload,
            f,
            indent=2
        )

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
    
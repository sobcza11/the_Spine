import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUT = (
    ROOT
    / "data"
    / "serving"
    / "cflow"
    / "cot_positioning_serving.json"
)

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_critical_minerals_serving.json"
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_score(payload):
    latest = payload.get("latest", {})

    return (
        latest.get("score")
        or latest.get("value")
        or latest.get("positioning_score")
        or 50.0
    )


def main():
    cot = load_json(INPUT)

    mineral_score = float(safe_score(cot))

    payload = {
        "metric": "GeoScen Critical Minerals",
        "category": "GeoScen",
        "sub_category": "Critical Minerals",
        "source": "the_Spine",
        "frequency": "Mixed",
        "meta": {
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "input_file": INPUT.name
        },
        "latest": {
            "score": mineral_score,
            "state": (
                "Stress"
                if mineral_score >= 60
                else "Watch"
            )
        },
        "minerals": [
            {
                "name": "Copper",
                "score": mineral_score
            },
            {
                "name": "Lithium",
                "score": mineral_score
            },
            {
                "name": "Nickel",
                "score": mineral_score
            },
            {
                "name": "Rare Earths",
                "score": mineral_score
            }
        ],
        "routing": {
            "P": round(mineral_score * 0.30, 2),
            "D": round(mineral_score * 0.30, 2),
            "S": round(mineral_score * 0.40, 2)
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

    
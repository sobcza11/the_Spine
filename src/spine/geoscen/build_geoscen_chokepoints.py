import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_chokepoints_serving.json"
)

CHOKEPOINTS = [
    {
        "name": "Strait of Hormuz",
        "importance": 95,
        "stress": 35
    },
    {
        "name": "Suez Canal",
        "importance": 90,
        "stress": 30
    },
    {
        "name": "Panama Canal",
        "importance": 85,
        "stress": 25
    },
    {
        "name": "South China Sea",
        "importance": 95,
        "stress": 40
    },
    {
        "name": "Strait of Malacca",
        "importance": 90,
        "stress": 35
    }
]


def main():

    avg_stress = round(
        sum(x["stress"] for x in CHOKEPOINTS)
        / len(CHOKEPOINTS),
        2
    )

    payload = {
        "metric": "GeoScen Chokepoints",
        "category": "GeoScen",
        "sub_category": "Strategic Chokepoints",
        "source": "the_Spine",
        "frequency": "On Build",
        "meta": {
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "count": len(CHOKEPOINTS)
        },
        "latest": {
            "score": avg_stress,
            "state": "Watch"
        },
        "chokepoints": CHOKEPOINTS,
        "routing": {
            "X": round(avg_stress * 0.60, 2),
            "S": round(avg_stress * 0.40, 2)
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

    
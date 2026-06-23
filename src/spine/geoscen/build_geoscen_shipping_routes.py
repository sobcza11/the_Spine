import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_shipping_routes_serving.json"
)

ROUTES = [
    {
        "route": "Strait of Hormuz",
        "region": "Middle East",
        "importance": 95,
        "stress": 35,
        "state": "Watch"
    },
    {
        "route": "Suez Canal",
        "region": "Egypt",
        "importance": 90,
        "stress": 30,
        "state": "Watch"
    },
    {
        "route": "Panama Canal",
        "region": "Central America",
        "importance": 85,
        "stress": 25,
        "state": "Watch"
    },
    {
        "route": "South China Sea",
        "region": "Asia Pacific",
        "importance": 95,
        "stress": 40,
        "state": "Watch"
    },
    {
        "route": "Strait of Malacca",
        "region": "Southeast Asia",
        "importance": 90,
        "stress": 35,
        "state": "Watch"
    }
]

def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    avg_stress = round(
        sum(r["stress"] for r in ROUTES) / len(ROUTES),
        2
    )

    payload = {
        "metric": "GeoScen Shipping Routes",
        "category": "GeoScen",
        "sub_category": "Shipping Routes",
        "source": "the_Spine",
        "frequency": "On Build",
        "meta": {
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "route_count": len(ROUTES)
        },
        "latest": {
            "route_count": len(ROUTES),
            "average_stress": avg_stress,
            "state": "Watch"
        },
        "routes": ROUTES,
        "routing": {
            "P": avg_stress,
            "X": avg_stress
        }
    }

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

    
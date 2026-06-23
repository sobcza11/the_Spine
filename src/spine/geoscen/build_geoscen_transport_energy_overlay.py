import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_transport_energy_overlay_serving.json"
)

REGIONS = [
    {
        "region": "North America",
        "resource": "Oil & Gas",
        "route": "Pipeline + Rail",
        "stress": "Watch"
    },
    {
        "region": "Middle East",
        "resource": "Crude Oil",
        "route": "Strait of Hormuz",
        "stress": "Watch"
    },
    {
        "region": "Europe",
        "resource": "Natural Gas",
        "route": "LNG",
        "stress": "Watch"
    },
    {
        "region": "Asia Pacific",
        "resource": "Container Shipping",
        "route": "South China Sea",
        "stress": "Watch"
    }
]


def main():
    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    payload = {
        "metric": "GeoScen Transport Energy Overlay",
        "category": "GeoScen",
        "sub_category": "Transport Energy Overlay",
        "source": "the_Spine",
        "frequency": "On Build",
        "meta": {
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "version": "1.0"
        },
        "latest": {
            "region_count": len(REGIONS),
            "status": "Active"
        },
        "regions": REGIONS,
        "routing": {
            "P": "Geographic Pressure",
            "D": "Regional Dispersion",
            "X": "Cross-Border Transmission",
            "S": "Systemic Geographic Stress"
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

    
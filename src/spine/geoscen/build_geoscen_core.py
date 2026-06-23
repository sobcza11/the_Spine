import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_core_serving.json"
)

DOMAINS = [
    "Energy Geography",
    "Shipping Routes",
    "Critical Minerals",
    "Food Supply",
    "Industrial Capacity",
    "Strategic Chokepoints",
    "Regional Stress"
]

def main():
    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    payload = {
        "metric": "GeoScen Core",
        "category": "GeoScen",
        "sub_category": "Core Registry",
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
            "active_domains": len(DOMAINS),
            "status": "Core Active"
        },
        "domains": DOMAINS,
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

    
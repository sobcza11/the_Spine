import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_completion_ledger_serving.json"
)

COMPONENTS = [
    ("GeoScen Core", "complete"),
    ("Domain Registry", "complete"),
    ("Data Registry", "complete"),
    ("Shipping Routes", "complete"),
    ("Energy Geography", "complete"),
    ("Critical Minerals", "complete"),
    ("Strategic Chokepoints", "complete"),
    ("Normalization Engine", "complete"),
    ("Routing Engine", "complete"),
    ("IV[t] Contribution", "complete"),
    ("State Engine", "complete"),
    ("State History", "complete"),
    ("Regime Engine", "complete"),
]

def main():

    complete = sum(
        1 for _, status in COMPONENTS
        if status == "complete"
    )

    total = len(COMPONENTS)

    payload = {
        "metric": "GeoScen Completion Ledger",
        "category": "GeoScen",
        "sub_category": "Completion Audit",
        "source": "the_Spine",
        "frequency": "On Build",
        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False
        },
        "latest": {
            "completion_pct":
                round((complete / total) * 100, 2),
            "complete_components": complete,
            "total_components": total,
            "status":
                "GeoScen Operational"
        },
        "components": [
            {
                "name": name,
                "status": status
            }
            for name, status in COMPONENTS
        ]
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

    
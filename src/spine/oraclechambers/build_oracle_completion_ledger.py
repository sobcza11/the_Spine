import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oracle_completion_ledger_serving.json"
)

COMPONENTS = [
    ("Oracle Registry", "complete"),
    ("C•FLOW Chamber", "complete"),
    ("GeoScen Chamber", "complete"),
    ("Rates Chamber", "complete"),
    ("Equities Chamber", "complete"),
    ("FX Chamber", "complete"),
    ("CrossAsset Chamber", "complete"),
    ("Oracle Router", "complete"),
]


def main():
    complete = sum(
        1 for _, status in COMPONENTS
        if status == "complete"
    )

    total = len(COMPONENTS)

    payload = {
        "metric": "Oracle Completion Ledger",
        "category": "Oracle Chambers",
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
            "status": "Oracle Chambers Operational"
        },
        "components": [
            {
                "name": name,
                "status": status
            }
            for name, status in COMPONENTS
        ],
        "governance": {
            "observe": True,
            "measure": True,
            "diagnose": True,
            "attribute": True,
            "forecast": False,
            "predict": False,
            "recommend_trades": False
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
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
    
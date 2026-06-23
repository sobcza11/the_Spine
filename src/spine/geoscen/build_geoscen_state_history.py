import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

STATE_ENGINE = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_state_engine_serving.json"
)

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_state_history_serving.json"
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():

    current = load_json(STATE_ENGINE)

    latest = current["latest"]

    history = [
        {
            "date": datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d"),
            "score": latest["score"],
            "state": latest["state"]
        }
    ]

    payload = {
        "metric": "GeoScen State History",
        "category": "GeoScen",
        "sub_category": "State History",
        "source": "the_Spine",
        "frequency": "Mixed",
        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False
        },
        "latest": {
            "record_count": len(history)
        },
        "history": history
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

    
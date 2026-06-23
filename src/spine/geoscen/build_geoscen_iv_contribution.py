import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_routing_engine_serving.json"
)

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_iv_contribution_serving.json"
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():

    routing = load_json(INPUT)

    latest = routing.get("latest", {})

    payload = {
        "metric": "GeoScen IV[t] Contribution",
        "category": "GeoScen",
        "sub_category": "IV Contribution",
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
            "active_vectors": 4,
            "coverage": "P,D,X,S"
        },
        "vectors": {
            "P": {
                "score": latest.get("P", 0.0),
                "source": "GeoScen"
            },
            "D": {
                "score": latest.get("D", 0.0),
                "source": "GeoScen"
            },
            "X": {
                "score": latest.get("X", 0.0),
                "source": "GeoScen"
            },
            "S": {
                "score": latest.get("S", 0.0),
                "source": "GeoScen"
            }
        },
        "attribution": {
            "routing_engine":
                "geoscen_routing_engine_serving.json"
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

    
import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

REGIME_FILE = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_regime_engine_serving.json"
)

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "geoscen_chamber_serving.json"
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():

    regime = load_json(REGIME_FILE)

    latest = regime["latest"]
    vectors = regime["vectors"]

    ranked = sorted(
        vectors.items(),
        key=lambda x: x[1],
        reverse=True
    )

    payload = {
        "metric": "GeoScen Chamber",
        "category": "Oracle Chambers",
        "sub_category": "GeoScen",
        "source": "the_Spine",
        "frequency": "On Build",

        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "chamber_version": "1.0"
        },

        "latest": {
            "regime": latest["regime"],
            "score": latest["score"],
            "state": latest["state"]
        },

        "observation":
            f"GeoScen currently classifies as "
            f"{latest['regime']} with a "
            f"diagnostic score of "
            f"{latest['score']}.",

        "measurement": [
            {
                "vector": k,
                "score": v
            }
            for k, v in ranked
        ],

        "diagnosis":
            "Cross-border transmission and "
            "geographic pressure are the "
            "largest active contributors.",

        "attribution": {
            "drivers": [
                {
                    "vector": ranked[0][0],
                    "score": ranked[0][1]
                },
                {
                    "vector": ranked[1][0],
                    "score": ranked[1][1]
                }
            ],

            "offsets": [
                {
                    "vector": ranked[-1][0],
                    "score": ranked[-1][1]
                },
                {
                    "vector": ranked[-2][0],
                    "score": ranked[-2][1]
                }
            ]
        },

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
        json.dump(
            payload,
            f,
            indent=2
        )

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
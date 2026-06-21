import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "cflow": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "cflow_chamber_serving.json"
    ),
    "geoscen": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "geoscen_chamber_serving.json"
    ),
    "rates": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "rates_chamber_serving.json"
    ),
    "equities": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "equities_chamber_serving.json"
    ),
    "fx": (
        ROOT
        / "data"
        / "serving"
        / "oraclechambers"
        / "fx_chamber_serving.json"
    ),
}

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "crossasset_chamber_serving.json"
)


WEIGHTS = {
    "cflow": 0.25,
    "rates": 0.20,
    "equities": 0.20,
    "fx": 0.20,
    "geoscen": 0.15,
}


def load_json(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def state_from_score(score):
    if score >= 75:
        return "Stress"
    if score >= 50:
        return "Watch"
    if score >= 25:
        return "Soft"
    return "Stable"


def get_score(payload):
    latest = payload.get("latest", {})

    return (
        latest.get("score")
        or latest.get("cflow_score")
        or latest.get("diagnostic_score")
        or latest.get("value")
    )


def main():

    chambers = {
        name: load_json(path)
        for name, path in INPUTS.items()
    }

    component_scores = {}

    for name, payload in chambers.items():
        score = get_score(payload)

        if score is not None:
            component_scores[name] = float(score)

    weighted_score = 0.0
    active_weight = 0.0

    for name, score in component_scores.items():
        weight = WEIGHTS.get(name, 0.0)
        weighted_score += score * weight
        active_weight += weight

    crossasset_score = round(
        weighted_score / active_weight,
        2
    )

    ranked = sorted(
        component_scores.items(),
        key=lambda x: x[1],
        reverse=True
    )

    state = state_from_score(crossasset_score)

    payload = {
        "metric": "CrossAsset Chamber",
        "category": "Oracle Chambers",
        "sub_category": "CrossAsset",
        "source": "the_Spine",
        "frequency": "Mixed",

        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "chamber_version": "1.0",
            "weights": WEIGHTS,
            "inputs": {
                name: str(path)
                for name, path in INPUTS.items()
            }
        },

        "latest": {
            "score": crossasset_score,
            "state": state,
            "active_chambers": len(component_scores),
            "total_chambers": len(INPUTS)
        },

        "observation":
            f"CrossAsset currently classifies as "
            f"{state} with a diagnostic score "
            f"of {crossasset_score}.",

        "measurement": [
            {
                "chamber": name,
                "score": score,
                "weight": WEIGHTS.get(name)
            }
            for name, score in ranked
        ],

        "diagnosis":
            f"{ranked[0][0]} is the largest "
            f"active cross-asset contributor.",

        "attribution": {
            "drivers": [
                {
                    "chamber": name,
                    "score": score,
                    "weight": WEIGHTS.get(name)
                }
                for name, score in ranked[:2]
            ],
            "offsets": [
                {
                    "chamber": name,
                    "score": score,
                    "weight": WEIGHTS.get(name)
                }
                for name, score in ranked[-2:]
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
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
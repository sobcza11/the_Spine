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
    / "geoscen_regime_engine_serving.json"
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def classify(score, P, D, X, S):

    if S >= 60:
        return "Constraint"

    if D >= 60 and X >= 50:
        return "Fragmentation"

    if P >= 60 and X >= 60:
        return "Stress"

    if score >= 55:
        return "Expansion"

    if score >= 40:
        return "Softening"

    return "Watch"


def main():

    state_engine = load_json(STATE_ENGINE)

    latest = state_engine["latest"]
    vectors = state_engine["vectors"]

    score = latest["score"]

    P = vectors["P"]
    D = vectors["D"]
    X = vectors["X"]
    S = vectors["S"]

    regime = classify(
        score=score,
        P=P,
        D=D,
        X=X,
        S=S
    )

    payload = {
        "metric": "GeoScen Regime Engine",
        "category": "GeoScen",
        "sub_category": "Regime Engine",
        "source": "the_Spine",
        "frequency": "Mixed",
        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "allowed_regimes": [
                "Expansion",
                "Softening",
                "Watch",
                "Stress",
                "Fragmentation",
                "Constraint"
            ]
        },
        "latest": {
            "score": score,
            "state": latest["state"],
            "regime": regime
        },
        "vectors": {
            "P": P,
            "D": D,
            "X": X,
            "S": S
        },
        "attribution": {
            "primary_driver": max(
                {
                    "P": P,
                    "D": D,
                    "X": X,
                    "S": S
                },
                key=lambda k: {
                    "P": P,
                    "D": D,
                    "X": X,
                    "S": S
                }[k]
            )
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

    
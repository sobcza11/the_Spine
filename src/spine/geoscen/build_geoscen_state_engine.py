import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_iv_contribution_serving.json"
)

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_state_engine_serving.json"
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def classify(score):
    if score >= 70:
        return "Stress"
    if score >= 50:
        return "Watch"
    if score >= 30:
        return "Soft"
    return "Stable"


def main():

    iv = load_json(INPUT)

    vectors = iv["vectors"]

    P = vectors["P"]["score"]
    D = vectors["D"]["score"]
    X = vectors["X"]["score"]
    S = vectors["S"]["score"]

    score = round(
        (
            P * 0.30 +
            D * 0.20 +
            X * 0.30 +
            S * 0.20
        ),
        2
    )

    state = classify(score)

    payload = {
        "metric": "GeoScen State Engine",
        "category": "GeoScen",
        "sub_category": "State Engine",
        "source": "the_Spine",
        "frequency": "Mixed",
        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "method":
                "geoscen_iv_contribution_v1"
        },
        "latest": {
            "score": score,
            "state": state
        },
        "vectors": {
            "P": P,
            "D": D,
            "X": X,
            "S": S
        },
        "attribution": {
            "Pressure": P,
            "Dispersion": D,
            "CrossAssetTransmission": X,
            "Systemicity": S
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

    
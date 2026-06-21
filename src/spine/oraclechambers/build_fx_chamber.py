import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "fx_depth": (
        ROOT
        / "data"
        / "serving"
        / "fx"
        / "fx_depth_serving_v1.json"
    ),
    "fx_latest": (
        ROOT
        / "data"
        / "serving"
        / "fx"
        / "fx_latest.json"
    ),
    "fx_sigma": (
        ROOT
        / "data"
        / "serving"
        / "fx"
        / "fx_sigma_data.json"
    ),
}

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "fx_chamber_serving.json"
)


def load_json(path, required=True):
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Missing input: {path}")
        return None

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clamp(value, low=0.0, high=100.0):
    return max(low, min(high, value))


def abs_to_score(value, multiplier=20.0):
    if value is None:
        return None

    return round(
        clamp(abs(float(value)) * multiplier),
        2
    )


def z_to_stress_score(value):
    if value is None:
        return None

    return round(
        clamp(50.0 + abs(float(value)) * 20.0),
        2
    )


def state_from_score(score):
    if score >= 75:
        return "Stress"
    if score >= 50:
        return "Watch"
    if score >= 25:
        return "Soft"
    return "Stable"


def average(values):
    values = [
        v for v in values
        if v is not None
    ]

    if not values:
        return None

    return sum(values) / len(values)


def extract_depth_scores(fx_depth):
    pairs = fx_depth.get("pairs", {})

    scores = []
    latest_rows = []

    for pair, payload in pairs.items():
        latest = payload.get("latest", {})
        value = latest.get("value")
        change = latest.get("change")

        value_score = abs_to_score(
            value,
            multiplier=20.0
        )

        change_score = abs_to_score(
            change,
            multiplier=250.0
        )

        pair_score = average(
            [
                value_score,
                change_score
            ]
        )

        if pair_score is not None:
            scores.append(pair_score)

        latest_rows.append(
            {
                "pair": pair,
                "metric": latest.get("metric"),
                "date": latest.get("date"),
                "value": value,
                "change": change,
                "score": round(pair_score, 2)
                if pair_score is not None
                else None
            }
        )

    return scores, latest_rows


def extract_sigma_score(fx_sigma):
    if not fx_sigma:
        return None

    if isinstance(fx_sigma, dict):
        for key in [
            "latest",
            "rows",
            "data"
        ]:
            node = fx_sigma.get(key)

            if isinstance(node, list) and node:
                values = []

                for row in node:
                    for field in [
                        "sigma",
                        "z",
                        "sigma_rank",
                        "score"
                    ]:
                        if field in row:
                            values.append(
                                z_to_stress_score(row.get(field))
                            )

                return average(values)

        for field in [
            "sigma",
            "z",
            "sigma_rank",
            "score"
        ]:
            if field in fx_sigma:
                return z_to_stress_score(fx_sigma.get(field))

    if isinstance(fx_sigma, list):
        values = []

        for row in fx_sigma:
            if not isinstance(row, dict):
                continue

            for field in [
                "sigma",
                "z",
                "sigma_rank",
                "score"
            ]:
                if field in row:
                    values.append(
                        z_to_stress_score(row.get(field))
                    )

        return average(values)

    return None


def main():

    fx_depth = load_json(INPUTS["fx_depth"])
    fx_latest = load_json(
        INPUTS["fx_latest"],
        required=False
    )
    fx_sigma = load_json(
        INPUTS["fx_sigma"],
        required=False
    )

    depth_scores, latest_pairs = extract_depth_scores(
        fx_depth
    )

    depth_score = round(
        average(depth_scores),
        2
    )

    strongest_pairs = sorted(
        [
            r for r in latest_pairs
            if r.get("score") is not None
        ],
        key=lambda x: x["score"],
        reverse=True
    )

    sigma_score_raw = extract_sigma_score(
        fx_sigma
    )

    sigma_score = (
        round(sigma_score_raw, 2)
        if sigma_score_raw is not None
        else None
    )

    dollar_pressure_score = (
        strongest_pairs[0]["score"]
        if strongest_pairs
        else None
    )

    cross_pair_dispersion_score = (
        round(
            max(depth_scores) - min(depth_scores),
            2
        )
        if depth_scores
        else None
    )

    components = {
        "FX Depth Pressure":
            depth_score,
        "Dollar Pressure Proxy":
            dollar_pressure_score,
        "Cross-Pair Dispersion":
            cross_pair_dispersion_score,
        "FX Sigma Stress":
            sigma_score,
    }

    valid_components = {
        k: v for k, v in components.items()
        if v is not None
    }

    chamber_score = round(
        sum(valid_components.values())
        / len(valid_components),
        2
    )

    ranked = sorted(
        valid_components.items(),
        key=lambda x: x[1],
        reverse=True
    )

    state = state_from_score(chamber_score)

    payload = {
        "metric": "FX Chamber",
        "category": "Oracle Chambers",
        "sub_category": "FX",
        "source": "the_Spine",
        "frequency": "Mixed",

        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "chamber_version": "1.0",
            "inputs": {
                "fx_depth":
                    str(INPUTS["fx_depth"]),
                "fx_latest":
                    str(INPUTS["fx_latest"]),
                "fx_sigma":
                    str(INPUTS["fx_sigma"]),
            }
        },

        "latest": {
            "date":
                strongest_pairs[0].get("date")
                if strongest_pairs
                else None,
            "score": chamber_score,
            "state": state,
            "pair_count": len(latest_pairs),
        },

        "observation":
            f"FX currently classifies as "
            f"{state} with a diagnostic "
            f"score of {chamber_score}.",

        "measurement": [
            {
                "component": name,
                "score": score
            }
            for name, score in ranked
        ],

        "diagnosis":
            f"{ranked[0][0]} is the largest "
            f"active FX contributor.",

        "attribution": {
            "drivers": [
                {
                    "component": name,
                    "score": score
                }
                for name, score in ranked[:2]
            ],
            "offsets": [
                {
                    "component": name,
                    "score": score
                }
                for name, score in ranked[-2:]
            ],
            "strongest_pairs":
                strongest_pairs[:5],
            "fx_latest":
                fx_latest
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
import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "yield_family": (
        ROOT
        / "data"
        / "serving"
        / "rates"
        / "rates_us_yield_family_latest.json"
    ),
    "zt_latest": (
        ROOT
        / "data"
        / "serving"
        / "rates"
        / "rates_zt_latest.json"
    ),
}

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "rates_chamber_serving.json"
)


def load_json(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clamp(value, low=0.0, high=100.0):
    return max(low, min(high, value))


def z_to_score(z_value):
    if z_value is None:
        return None

    return round(
        clamp(50.0 + float(z_value) * 20.0),
        2
    )


def spread_to_curve_score(spread):
    if spread is None:
        return None

    spread = float(spread)

    return round(
        clamp(50.0 + spread * 40.0),
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


def main():

    yield_family_raw = load_json(INPUTS["yield_family"])
    zt = load_json(INPUTS["zt_latest"])

    yield_family = (
        yield_family_raw[0]
        if isinstance(yield_family_raw, list)
        else yield_family_raw
    )

    curve_score = spread_to_curve_score(
        yield_family.get("us_10y_2y_spread")
    )

    policy_score = z_to_score(
        zt.get("policy_component")
    )

    real_yield_score = z_to_score(
        zt.get("real_component")
    )

    treasury_stress_score = z_to_score(
        zt.get("risk_component")
    )

    global_score = z_to_score(
        zt.get("global_component")
    )

    components = {
        "Curve Structure": curve_score,
        "Policy Restriction": policy_score,
        "Real Yield Pressure": real_yield_score,
        "Treasury Stress": treasury_stress_score,
        "Global Rates Dispersion": global_score,
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
        "metric": "Rates Chamber",
        "category": "Oracle Chambers",
        "sub_category": "Rates",
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
                "yield_family":
                    str(INPUTS["yield_family"]),
                "zt_latest":
                    str(INPUTS["zt_latest"]),
            }
        },

        "latest": {
            "date": yield_family.get(
                "date",
                zt.get("date")
            ),
            "score": chamber_score,
            "state": state,
            "rates_zt_state":
                zt.get("rates_zt_state"),
            "rates_zt":
                zt.get("rates_zt"),
        },

        "observation":
            f"Rates currently classify as "
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
            f"active rates contributor.",

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
            "raw": {
                "us_2y_yield":
                    yield_family.get("us_2y_yield"),
                "us_10y_yield":
                    yield_family.get("us_10y_yield"),
                "us_30y_yield":
                    yield_family.get("us_30y_yield"),
                "us_10y_2y_spread":
                    yield_family.get("us_10y_2y_spread"),
                "policy_component":
                    zt.get("policy_component"),
                "real_component":
                    zt.get("real_component"),
                "risk_component":
                    zt.get("risk_component"),
                "global_component":
                    zt.get("global_component"),
                "term_premium_10y":
                    zt.get("term_premium_10y"),
                "real_y10":
                    zt.get("real_y10"),
            }
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
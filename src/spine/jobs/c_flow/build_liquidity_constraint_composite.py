from pathlib import Path
import json
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[4]

CAPITAL_FILE = (
    ROOT
    / "data/serving/cflow"
    / "capital_composite_serving.json"
)

CREDIT_FILE = (
    ROOT
    / "data/serving/cflow"
    / "credit_transmission_composite_serving.json"
)

OUT_FILE = (
    ROOT
    / "data/serving/cflow"
    / "liquidity_constraint_composite_serving.json"
)


def load_json(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def clamp(x):
    return max(0.0, min(100.0, float(x)))


def classify(score):
    if score < 30:
        return "Benign"
    if score < 50:
        return "Softening"
    if score < 70:
        return "Watch"
    if score < 85:
        return "Stress"
    return "Constraint"


def get_score(payload):
    latest = payload.get("latest", {})
    return (
        latest.get("score")
        or latest.get("value")
        or latest.get("transmission_score")
        or latest.get("systemicity_proxy")
        or 0
    )


capital = load_json(CAPITAL_FILE)
credit = load_json(CREDIT_FILE)

capital_score = clamp(get_score(capital))
credit_score = clamp(get_score(credit))

score = round(
    (capital_score * 0.60)
    + (credit_score * 0.40),
    2
)

state = classify(score)

payload = {
    "metric": "Liquidity Constraint Composite",
    "category": "C•FLOW",
    "sub_category": "Liquidity",
    "source": "the_Spine",
    "frequency": "Mixed",

    "meta": {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "forecasting": False,
        "prediction": False,
        "trade_recommendation": False,
        "method": "capital_plus_credit_v1",
        "inputs": {
            "capital_composite": str(CAPITAL_FILE),
            "credit_transmission_composite": str(CREDIT_FILE),
        },
    },

    "latest": {
        "score": score,
        "state": state,
    },

    "observation": (
        f"Liquidity constraint currently classifies as "
        f"{state} with a diagnostic score of {score}."
    ),

    "measurement": [
        {
            "component": "Capital Composite",
            "score": round(capital_score, 2),
        },
        {
            "component": "Credit Transmission Composite",
            "score": round(credit_score, 2),
        },
    ],

    "diagnosis": (
        "Capital conditions are the largest active liquidity contributor."
        if capital_score >= credit_score
        else "Credit transmission is the largest active liquidity contributor."
    ),

    "attribution": {
        "drivers": [
            {
                "component": (
                    "Capital Composite"
                    if capital_score >= credit_score
                    else "Credit Transmission Composite"
                ),
                "score": round(max(capital_score, credit_score), 2),
            }
        ],
        "offsets": [
            {
                "component": (
                    "Credit Transmission Composite"
                    if capital_score >= credit_score
                    else "Capital Composite"
                ),
                "score": round(min(capital_score, credit_score), 2),
            }
        ],
        "raw": {
            "capital_score": capital_score,
            "credit_score": credit_score,
        },
    },

    "governance": {
        "observe": True,
        "measure": True,
        "diagnose": True,
        "attribute": True,
        "forecast": False,
        "predict": False,
        "recommend_trades": False,
    },
}

OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Wrote {OUT_FILE}")
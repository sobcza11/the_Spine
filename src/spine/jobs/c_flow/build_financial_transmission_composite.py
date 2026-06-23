from pathlib import Path
import json
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[4]

CREDIT_FILE = (
    ROOT
    / "data/serving/cflow"
    / "credit_transmission_composite_serving.json"
)

LIQUIDITY_FILE = (
    ROOT
    / "data/serving/cflow"
    / "liquidity_constraint_composite_serving.json"
)

CAPITAL_FILE = (
    ROOT
    / "data/serving/cflow"
    / "capital_composite_serving.json"
)

OUT_FILE = (
    ROOT
    / "data/serving/cflow"
    / "financial_transmission_composite_serving.json"
)


def load_json(path):
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


credit = load_json(CREDIT_FILE)
liquidity = load_json(LIQUIDITY_FILE)
capital = load_json(CAPITAL_FILE)

credit_score = clamp(get_score(credit))
liquidity_score = clamp(get_score(liquidity))
capital_score = clamp(get_score(capital))

score = round(
    (credit_score * 0.40)
    + (liquidity_score * 0.35)
    + (capital_score * 0.25),
    2
)

state = classify(score)

ranked = sorted(
    [
        ("Credit Transmission", credit_score),
        ("Liquidity Constraint", liquidity_score),
        ("Capital Composite", capital_score),
    ],
    key=lambda x: x[1],
    reverse=True,
)

payload = {
    "metric": "Financial Transmission Composite",
    "category": "C•FLOW",
    "sub_category": "Financial Transmission",
    "source": "the_Spine",
    "frequency": "Mixed",

    "meta": {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "forecasting": False,
        "prediction": False,
        "trade_recommendation": False,
        "method": "credit_liquidity_capital_v1",
        "inputs": {
            "credit_transmission_composite": str(CREDIT_FILE),
            "liquidity_constraint_composite": str(LIQUIDITY_FILE),
            "capital_composite": str(CAPITAL_FILE),
        },
    },

    "latest": {
        "score": score,
        "state": state,
    },

    "observation": (
        f"Financial transmission currently classifies as "
        f"{state} with a diagnostic score of {score}."
    ),

    "measurement": [
        {
            "component": name,
            "score": round(component_score, 2),
        }
        for name, component_score in ranked
    ],

    "diagnosis": (
        f"{ranked[0][0]} is the largest active "
        f"financial transmission contributor."
    ),

    "attribution": {
        "drivers": [
            {
                "component": ranked[0][0],
                "score": round(ranked[0][1], 2),
            },
            {
                "component": ranked[1][0],
                "score": round(ranked[1][1], 2),
            },
        ],
        "offsets": [
            {
                "component": ranked[-1][0],
                "score": round(ranked[-1][1], 2),
            }
        ],
        "raw": {
            "credit_score": credit_score,
            "liquidity_score": liquidity_score,
            "capital_score": capital_score,
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


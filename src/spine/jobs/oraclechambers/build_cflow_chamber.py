import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "financial_transmission": ROOT / "data/serving/cflow/financial_transmission_composite_serving.json",
    "liquidity": ROOT / "data/serving/cflow/liquidity_constraint_composite_serving.json",
    "credit": ROOT / "data/serving/cflow/credit_transmission_composite_serving.json",
    "capital": ROOT / "data/serving/cflow/capital_composite_serving.json",
    "transport": ROOT / "data/serving/cflow/transport_transmission_composite_serving.json",
    "econ": ROOT / "data/serving/cflow/econ_composite_serving.json",
}

OUTPUT = ROOT / "data/serving/oraclechambers/cflow_chamber_serving.json"

WEIGHTS = {
    "financial_transmission": 0.25,
    "liquidity": 0.20,
    "credit": 0.20,
    "capital": 0.15,
    "transport": 0.10,
    "econ": 0.10,
}


def load_json(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_score(payload):
    latest = payload.get("latest", {})
    return (
        latest.get("score")
        or latest.get("cflow_score")
        or latest.get("value")
        or latest.get("transmission_score")
        or latest.get("systemicity_proxy")
        or 0
    )


def get_state(payload):
    latest = payload.get("latest", {})
    return latest.get("state") or latest.get("regime") or "Unknown"


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


def main():
    payloads = {
        name: load_json(path)
        for name, path in INPUTS.items()
    }

    scores = {
        name: float(get_score(payload))
        for name, payload in payloads.items()
    }

    score = round(
        sum(scores[k] * WEIGHTS[k] for k in scores)
        / sum(WEIGHTS[k] for k in scores),
        2,
    )

    state = classify(score)

    ranked = sorted(
        scores.items(),
        key=lambda x: x[1],
        reverse=True,
    )

    out = {
        "metric": "C•FLOW Chamber",
        "category": "Oracle Chambers",
        "sub_category": "C•FLOW",
        "source": "the_Spine",
        "frequency": "Mixed",
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False,
            "chamber_version": "2.0",
            "method": "cflow_transmission_chamber_v2",
            "weights": WEIGHTS,
            "inputs": {k: str(v) for k, v in INPUTS.items()},
        },
        "latest": {
            "score": score,
            "state": state,
            "active_components": len(scores),
            "total_components": len(INPUTS),
        },
        "observation": (
            f"C•FLOW currently classifies as {state} "
            f"with a diagnostic score of {score}."
        ),
        "measurement": [
            {
                "component": name,
                "score": round(component_score, 2),
                "state": get_state(payloads[name]),
                "weight": WEIGHTS[name],
            }
            for name, component_score in ranked
        ],
        "diagnosis": (
            f"{ranked[0][0]} is the largest active C•FLOW contributor."
        ),
        "attribution": {
            "drivers": [
                {
                    "component": name,
                    "score": round(component_score, 2),
                    "weight": WEIGHTS[name],
                }
                for name, component_score in ranked[:3]
            ],
            "offsets": [
                {
                    "component": name,
                    "score": round(component_score, 2),
                    "weight": WEIGHTS[name],
                }
                for name, component_score in ranked[-3:]
            ],
            "components": [
                {
                    "component": name,
                    "score": round(component_score, 2),
                    "state": get_state(payloads[name]),
                    "metric": payloads[name].get("metric"),
                }
                for name, component_score in ranked
            ],
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

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
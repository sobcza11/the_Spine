from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

FINANCIAL = ROOT / "data/serving/c_flow/financial_transmission_composite_serving.json"
LIQUIDITY = ROOT / "data/serving/c_flow/liquidity_constraint_composite_serving.json"
FUNDING = ROOT / "data/serving/c_flow/funding_stress_composite_serving.json"
CREDIT = ROOT / "data/serving/c_flow/credit_transmission_composite_serving.json"
COT = ROOT / "data/serving/cflow/cot_positioning_serving.json"
CAPITAL = ROOT / "data/serving/cflow/capital_composite_serving.json"

OUTPUT = ROOT / "data/serving/cflow/capital_composite_serving.json"


def load_latest(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    latest = payload["latest"]
    score = float(latest.get("score", latest.get("value", 0)))

    if score <= 1:
        score *= 100

    return {
        "date": str(latest.get("date", "")),
        "score": score,
        "state": str(latest.get("state", "unknown")),
    }


def classify(score: float) -> str:
    if score < 25:
        return "capital_benign"
    if score < 50:
        return "capital_watch"
    if score < 75:
        return "capital_stress"
    return "capital_constraint"


def main():
    inputs = {
        "financial_transmission": {"latest": load_latest(FINANCIAL), "weight": 0.30},
        "liquidity_constraint": {"latest": load_latest(LIQUIDITY), "weight": 0.25},
        "funding_stress": {"latest": load_latest(FUNDING), "weight": 0.20},
        "credit_transmission": {"latest": load_latest(CREDIT), "weight": 0.15},
        "cot_positioning": {"latest": load_latest(COT), "weight": 0.10},
    }

    score = round(
        sum(v["latest"]["score"] * v["weight"] for v in inputs.values()),
        2,
    )

    latest_date = max(v["latest"]["date"] for v in inputs.values())

    capital_latest, _ = load_latest(CAPITAL)
    capital_score = float(capital_latest["score"])

    out = {
        "metric": "Capital Composite",
        "category": "Capital",
        "sub_category": "Composite",
        "source": "the_Spine | C•FLOW Capital",
        "frequency": "Mixed",
        "meta": {
            "name": "Capital Composite",
            "forecasting": "prohibited",
            "phase": "8P",
            "method": "capital_weighted_component_score_v1",
        },
        "latest": {
            "date": latest_date,
            "value": score,
            "score": score,
            "state": classify(score),
            "bias": "diagnostic_only",
        },
        "attribution": {
            k: {
                "date": v["latest"]["date"],
                "score": v["latest"]["score"],
                "state": v["latest"]["state"],
                "weight": v["weight"],
            }
            for k, v in inputs.items()
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
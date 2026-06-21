from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

CREDIT = ROOT / "data/serving/c_flow/credit_transmission_composite_serving.json"
LIQUIDITY = ROOT / "data/serving/c_flow/liquidity_constraint_composite_serving.json"
CAPITAL = ROOT / "data/serving/cflow/capital_composite_serving.json"

OUTPUT = ROOT / "data/serving/cflow/fragility_composite_serving.json"


def load_latest(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    latest = payload["latest"]

    score = float(latest.get("score", latest.get("value", 0)))

    if score <= 1:
        score *= 100

    return {
        "date": str(latest.get("date", "")),
        "value": score,
        "score": score,
        "state": str(latest.get("state", "unknown")),
        "bias": str(latest.get("bias", "diagnostic_only")),
    }


def classify(score: float) -> str:
    if score < 25:
        return "fragility_low"
    if score < 50:
        return "fragility_watch"
    if score < 75:
        return "fragility_stress"
    return "fragility_constraint"


def main():
    credit = load_latest(CREDIT)
    liquidity = load_latest(LIQUIDITY)
    capital = load_latest(CAPITAL)

    score = round(
        (0.40 * credit["score"])
        + (0.35 * liquidity["score"])
        + (0.25 * capital["score"]),
        2,
    )

    latest_date = max(
        credit["date"],
        liquidity["date"],
        capital["date"],
    )

    out = {
        "metric": "Fragility Composite",
        "category": "IV[t]",
        "sub_category": "Structural Fragility",
        "source": "the_Spine | C•FLOW structural fragility",
        "frequency": "Mixed",
        "meta": {
            "name": "Fragility Composite",
            "forecasting": "prohibited",
            "phase": "8Q",
            "method": "credit_liquidity_capital_fragility_v1",
            "iv_target": "F",
        },
        "latest": {
            "date": latest_date,
            "value": score,
            "score": score,
            "state": classify(score),
            "bias": "diagnostic_only",
        },
        "attribution": {
            "credit_transmission": {
                "date": credit["date"],
                "score": credit["score"],
                "state": credit["state"],
                "weight": 0.40,
            },
            "liquidity_constraint": {
                "date": liquidity["date"],
                "score": liquidity["score"],
                "state": liquidity["state"],
                "weight": 0.35,
            },
            "capital_composite": {
                "date": capital["date"],
                "score": capital["score"],
                "state": capital["state"],
                "weight": 0.25,
            },
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
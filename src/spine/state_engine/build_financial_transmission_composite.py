from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

FUNDING = ROOT / "data/serving/c_flow/funding_stress_composite_serving.json"
CREDIT = ROOT / "data/serving/c_flow/credit_transmission_composite_serving.json"
LIQUIDITY = ROOT / "data/serving/c_flow/liquidity_constraint_composite_serving.json"
COT = ROOT / "data/serving/cflow/cot_positioning_serving.json"

OUTPUT = ROOT / "data/serving/c_flow/financial_transmission_composite_serving.json"


def load_score(path: Path):
    payload = json.loads(path.read_text())
    latest = payload["latest"]
    return (
        str(latest["date"]),
        float(latest.get("score", latest.get("value", 0))),
        str(latest.get("state", "unknown")),
    )


def classify(score: float) -> str:
    if score < 25:
        return "Benign"
    if score < 50:
        return "Watch"
    if score < 75:
        return "Stress"
    return "Constraint"


def main():
    funding_date, funding_raw_score, funding_state = load_score(FUNDING)
    credit_date, credit_score, credit_state = load_score(CREDIT)
    liquidity_date, liquidity_score, liquidity_state = load_score(LIQUIDITY)
    cot_date, cot_score, cot_state = load_score(COT)

    if funding_raw_score <= 1:
        funding_raw_score *= 100

    funding_constraint_score = 100 - funding_raw_score

    financial_score = round(
        (0.30 * funding_constraint_score)
        + (0.30 * credit_score)
        + (0.30 * liquidity_score)
        + (0.10 * cot_score),
        2,
    )

    latest_date = max(funding_date, credit_date, liquidity_date, cot_date)

    out = {
        "metric": "Financial Transmission Composite",
        "category": "Financial Transmission",
        "sub_category": "Composite",
        "source": "the_Spine | C•FLOW financial transmission",
        "frequency": "Mixed",
        "meta": {
            "name": "Financial Transmission Composite",
            "ft_gmi_role": "Financial Transmission",
            "forecasting": "prohibited",
            "phase": "8D",
            "method": "weighted_component_score_v1",
        },
        "latest": {
            "date": latest_date,
            "value": financial_score,
            "score": financial_score,
            "state": classify(financial_score),
            "bias": "diagnostic_only",
        },
        "attribution": {
            "funding_constraint": {
                "date": funding_date,
                "raw_score": funding_raw_score,
                "constraint_score": funding_constraint_score,
                "state": funding_state,
                "weight": 0.30,
            },
            "credit_transmission": {
                "date": credit_date,
                "score": credit_score,
                "state": credit_state,
                "weight": 0.30,
            },
            "liquidity_constraint": {
                "date": liquidity_date,
                "score": liquidity_score,
                "state": liquidity_state,
                "weight": 0.30,
            },
            "cot_positioning_overlay": {
                "date": cot_date,
                "score": cot_score,
                "state": cot_state,
                "weight": 0.10,
            },
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2))

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()


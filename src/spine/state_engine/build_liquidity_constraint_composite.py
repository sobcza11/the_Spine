from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

FUNDING = ROOT / "data/serving/c_flow/funding_stress_composite_serving.json"
CREDIT = ROOT / "data/serving/c_flow/credit_transmission_composite_serving.json"
COT = ROOT / "data/serving/cflow/cot_positioning_serving.json"

OUTPUT = ROOT / "data/serving/c_flow/liquidity_constraint_composite_serving.json"


def load_score(path: Path) -> tuple[str, float, str]:
    payload = json.loads(path.read_text())
    latest = payload["latest"]
    date = str(latest["date"])
    score = float(latest.get("score", latest.get("value", 0)))

    # Normalize COT score if value is 0-1
    if score <= 1:
        score *= 100

    state = str(latest.get("state", "unknown"))
    return date, score, state


def classify(score: float) -> str:
    if score < 25:
        return "Benign"
    if score < 50:
        return "Watch"
    if score < 75:
        return "Stress"
    return "Constraint"


def main():
    funding_date, funding_score, funding_state = load_score(FUNDING)
    credit_date, credit_score, credit_state = load_score(CREDIT)
    cot_date, cot_score, cot_state = load_score(COT)

    funding_constraint_score = 100 - funding_score

    liquidity_score = round(
        (0.40 * funding_constraint_score)
        + (0.40 * credit_score)
        + (0.20 * cot_score),
        2,
    )

    latest_date = max(funding_date, credit_date, cot_date)

    out = {
        "metric": "Liquidity Constraint Composite",
        "category": "Financial Transmission",
        "sub_category": "Liquidity",
        "source": "the_Spine | C•FLOW financial transmission",
        "frequency": "Mixed",
        "meta": {
            "name": "Liquidity Constraint Composite",
            "ft_gmi_role": "Financial Transmission",
            "forecasting": "prohibited",
            "phase": "8C",
            "method": "weighted_component_score_v1",
        },
        "latest": {
            "date": latest_date,
            "value": liquidity_score,
            "score": liquidity_score,
            "state": classify(liquidity_score),
            "bias": "diagnostic_only",
        },
        "attribution": {
            "funding_stress_composite": {
                "date": funding_date,
                "raw_score": funding_score,
                "constraint_score": funding_constraint_score,
                "state": funding_state,
                "weight": 0.40,
            },
            "credit_transmission_composite": {
                "date": credit_date,
                "score": credit_score,
                "state": credit_state,
                "weight": 0.40,
            },
            "cot_positioning": {
                "date": cot_date,
                "score": cot_score,
                "state": cot_state,
                "weight": 0.20,
            },
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2))

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

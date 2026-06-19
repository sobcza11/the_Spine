from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

FINANCIAL = ROOT / "data/serving/c_flow/financial_transmission_composite_serving.json"
LIQUIDITY = ROOT / "data/serving/c_flow/liquidity_constraint_composite_serving.json"

OUTPUT = ROOT / "data/serving/c_flow/cflow_iv_vector_contribution_serving.json"


def load_latest(path: Path):
    payload = json.loads(path.read_text())
    return payload["latest"], payload


def classify(score: float) -> str:
    if score < 25:
        return "Benign"
    if score < 50:
        return "Watch"
    if score < 75:
        return "Stress"
    return "Constraint"


def main():
    financial_latest, financial_payload = load_latest(FINANCIAL)
    liquidity_latest, liquidity_payload = load_latest(LIQUIDITY)

    financial_score = float(financial_latest["score"])
    liquidity_score = float(liquidity_latest["score"])

    # First live IV[t] contribution from C•FLOW Financial Transmission.
    iv_l = round((0.70 * liquidity_score) + (0.30 * financial_score), 2)
    iv_x = round((0.60 * financial_score) + (0.40 * liquidity_score), 2)
    iv_s = round((0.50 * financial_score) + (0.50 * liquidity_score), 2)

    composite_score = round((iv_l + iv_x + iv_s) / 3, 2)

    latest_date = max(
        str(financial_latest["date"]),
        str(liquidity_latest["date"]),
    )

    out = {
        "metric": "C•FLOW IV[t] Vector Contribution",
        "category": "IV[t]",
        "sub_category": "C•FLOW",
        "source": "the_Spine | C•FLOW state engine",
        "frequency": "Mixed",
        "meta": {
            "name": "C•FLOW IV[t] Vector Contribution",
            "forecasting": "prohibited",
            "phase": "8H",
            "method": "vector_routing_v1",
        },
        "latest": {
            "date": latest_date,
            "value": composite_score,
            "score": composite_score,
            "state": classify(composite_score),
            "bias": "diagnostic_only",
        },
        "iv_vector": {
            "L": {
                "name": "Liquidity",
                "score": iv_l,
                "state": classify(iv_l),
                "source_weighting": {
                    "liquidity_constraint": 0.70,
                    "financial_transmission": 0.30,
                },
            },
            "X": {
                "name": "Cross-Market Stress",
                "score": iv_x,
                "state": classify(iv_x),
                "source_weighting": {
                    "financial_transmission": 0.60,
                    "liquidity_constraint": 0.40,
                },
            },
            "S": {
                "name": "Systemicity",
                "score": iv_s,
                "state": classify(iv_s),
                "source_weighting": {
                    "financial_transmission": 0.50,
                    "liquidity_constraint": 0.50,
                },
            },
        },
        "attribution": {
            "financial_transmission_composite": financial_latest,
            "liquidity_constraint_composite": liquidity_latest,
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2))

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
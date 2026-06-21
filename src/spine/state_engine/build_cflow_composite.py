from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

ECON = ROOT / "data/serving/cflow/econ_composite_serving.json"
FINANCIAL = ROOT / "data/serving/c_flow/financial_transmission_composite_serving.json"

OUTPUT = ROOT / "data/serving/cflow/cflow_composite_serving.json"


def load_metric(path: Path):
    payload = json.loads(path.read_text())

    latest = payload["latest"]

    return {
        "date": latest["date"],
        "score": float(latest["score"]),
        "state": latest["state"],
    }


def classify(score: float):

    if score < 25:
        return "Soft"

    if score < 50:
        return "Watch"

    if score < 75:
        return "Stress"

    return "Constraint"


def main():

    econ = load_metric(ECON)
    financial = load_metric(FINANCIAL)

    composite_score = round(
        (econ["score"] * 0.50)
        +
        (financial["score"] * 0.50),
        2,
    )

    latest_date = max(
        econ["date"],
        financial["date"],
    )

    payload = {
        "metric": "C•FLOW Composite",
        "category": "C•FLOW",
        "sub_category": "Composite",
        "source": "the_Spine | C•FLOW state engine",
        "frequency": "Mixed",

        "meta": {
            "name": "C•FLOW Composite",
            "forecasting": "prohibited",
            "phase": "8M",
            "method": "econ_financial_equal_weight_v1",
        },

        "latest": {
            "date": latest_date,
            "value": composite_score,
            "score": composite_score,
            "state": classify(composite_score),
            "bias": "diagnostic_only",
        },

        "attribution": {

            "econ_composite": {
                "date": econ["date"],
                "score": econ["score"],
                "state": econ["state"],
                "weight": 0.50,
            },

            "financial_transmission_composite": {
                "date": financial["date"],
                "score": financial["score"],
                "state": financial["state"],
                "weight": 0.50,
            },
        },
    }

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    OUTPUT.write_text(
        json.dumps(payload, indent=2)
    )

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
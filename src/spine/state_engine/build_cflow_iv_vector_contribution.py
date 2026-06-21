from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

FINANCIAL = ROOT / "data/serving/c_flow/financial_transmission_composite_serving.json"
LIQUIDITY = ROOT / "data/serving/c_flow/liquidity_constraint_composite_serving.json"
TRANSPORT = ROOT / "data/serving/cflow/transport_transmission_composite_serving.json"
ECON = ROOT / "data/serving/cflow/econ_composite_serving.json"
CAPITAL = ROOT / "data/serving/cflow/capital_composite_serving.json"

FRAGILITY = ROOT / "data/serving/cflow/fragility_composite_serving.json"
DISPERSION = ROOT / "data/serving/cflow/dispersion_composite_serving.json"

OUTPUT = ROOT / "data/serving/c_flow/cflow_iv_vector_contribution_serving.json"


def load_latest(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
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
    financial_latest, _ = load_latest(FINANCIAL)
    liquidity_latest, _ = load_latest(LIQUIDITY)
    transport_latest, _ = load_latest(TRANSPORT)
    econ_latest, _ = load_latest(ECON)
    capital_latest, _ = load_latest(CAPITAL)

    fragility_latest, _ = load_latest(FRAGILITY)
    dispersion_latest, _ = load_latest(DISPERSION)

    fragility_score = float(fragility_latest["score"])
    dispersion_score = float(dispersion_latest["score"])

    financial_score = float(financial_latest["score"])
    liquidity_score = float(liquidity_latest["score"])
    transport_score = float(transport_latest["score"])
    econ_score = float(econ_latest["score"])
    capital_score = float(capital_latest["score"])

    iv_p = round((0.60 * transport_score) + (0.40 * econ_score), 2)
    iv_m = round((0.50 * transport_score) + (0.50 * econ_score), 2)
    iv_l = round((0.70 * liquidity_score) + (0.30 * financial_score), 2)

    iv_f = fragility_score
    iv_d = dispersion_score

    iv_x = round((0.50 * transport_score) + (0.50 * financial_score), 2)

    iv_c = round(
        (0.50 * capital_score)
        + (0.30 * financial_score)
        + (0.20 * liquidity_score),
        2,
    )

    iv_s = round(
        (0.40 * econ_score)
        + (0.40 * financial_score)
        + (0.20 * transport_score),
        2,
    )

    composite_score = round((iv_p + iv_f + iv_l + iv_d + iv_m + iv_x + iv_c + iv_s) / 8, 2)


    latest_date = max(
        str(financial_latest["date"]),
        str(liquidity_latest["date"]),
        str(transport_latest["date"]),
        str(econ_latest["date"]),
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
            "phase": "8O",
            "method": "vector_routing_transport_econ_capital_v1",
        },
        "latest": {
            "date": latest_date,
            "value": composite_score,
            "score": composite_score,
            "state": classify(composite_score),
            "bias": "diagnostic_only",
        },
        "iv_vector": {
            "P": {
                "name": "Pressure",
                "score": iv_p,
                "state": classify(iv_p),
                "source_weighting": {
                    "transport_transmission": 0.60,
                    "econ_composite": 0.40,
                },
            },
            "M": {
                "name": "Momentum",
                "score": iv_m,
                "state": classify(iv_m),
                "source_weighting": {
                    "transport_transmission": 0.50,
                    "econ_composite": 0.50,
                },
            },
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
                    "transport_transmission": 0.50,
                    "financial_transmission": 0.50,
                },
            },
            "C": {
                "name": "Coherence",
                "score": iv_c,
                "state": classify(iv_c),
                "source_weighting": {
                    "capital_composite": 0.50,
                    "financial_transmission": 0.30,
                    "liquidity_constraint": 0.20,
                },
            },

            "F": {
                "name": "Fragility",
                "score": iv_f,
                "state": classify(iv_f),
                "source_weighting": {
                    "fragility_composite": 1.0
                }
            },

            "D": {
                "name": "Dispersion",
                "score": iv_d,
                "state": classify(iv_d),
                "source_weighting": {
                    "dispersion_composite": 1.0
                }
            },


            "S": {
                "name": "Systemicity",
                "score": iv_s,
                "state": classify(iv_s),
                "source_weighting": {
                    "econ_composite": 0.40,
                    "financial_transmission": 0.40,
                    "transport_transmission": 0.20,
                },
            },
        },
        "attribution": {
            "financial_transmission_composite": financial_latest,
            "liquidity_constraint_composite": liquidity_latest,
            "transport_transmission_composite": transport_latest,
            "econ_composite": econ_latest,
            "capital_composite": capital_latest,
            "fragility_composite": fragility_latest,
            "dispersion_composite": dispersion_latest,
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()
    
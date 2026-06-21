from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

CFLOW_COMPOSITE = ROOT / "data/serving/cflow/cflow_composite_serving.json"
ECON_COMPOSITE = ROOT / "data/serving/cflow/econ_composite_serving.json"
CAPITAL_COMPOSITE = ROOT / "data/serving/cflow/capital_composite_serving.json"
FINANCIAL_COMPOSITE = ROOT / "data/serving/c_flow/financial_transmission_composite_serving.json"
IV_VECTOR = ROOT / "data/serving/c_flow/cflow_iv_vector_contribution_serving.json"

OUTPUT = ROOT / "data/serving/cflow/cflow_state_engine_serving.json"


def load_latest(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    latest = payload["latest"]

    return {
        "date": str(latest.get("date", "")),
        "value": float(latest.get("value", latest.get("score", 0))),
        "score": float(latest.get("score", latest.get("value", 0))),
        "state": str(latest.get("state", "unknown")),
        "bias": str(latest.get("bias", "diagnostic_only")),
    }


def classify(score: float) -> str:
    if score < 25:
        return "Soft"
    if score < 50:
        return "Watch"
    if score < 75:
        return "Stress"
    return "Constraint"


def main():
    cflow = load_latest(CFLOW_COMPOSITE)
    econ = load_latest(ECON_COMPOSITE)
    capital = load_latest(CAPITAL_COMPOSITE)
    financial = load_latest(FINANCIAL_COMPOSITE)

    iv_payload = json.loads(IV_VECTOR.read_text(encoding="utf-8"))
    iv_latest = iv_payload["latest"]
    iv_vector = iv_payload.get("iv_vector", {})

    active_vectors = {
        key: val
        for key, val in iv_vector.items()
        if val.get("score") is not None
    }

    vector_scores = [
        float(val["score"])
        for val in active_vectors.values()
        if val.get("score") is not None
    ]

    vector_score = round(sum(vector_scores) / len(vector_scores), 2)

    latest_date = max(
        cflow["date"],
        econ["date"],
        capital["date"],
        financial["date"],
        str(iv_latest["date"]),
    )

    score = round(
        (0.60 * cflow["score"])
        + (0.40 * vector_score),
        2,
    )

    out = {
        "metric": "C•FLOW State Engine",
        "category": "C•FLOW",
        "sub_category": "State Engine",
        "source": "the_Spine | C•FLOW state engine",
        "frequency": "Mixed",
        "meta": {
            "name": "C•FLOW State Engine",
            "forecasting": "prohibited",
            "phase": "8S",
            "method": "cflow_composite_plus_full_iv_vector_v1",
            "state_source": "cflow_composite + iv_vector_8of8",
            "active_vector_count": len(active_vectors),
            "target_vector_count": 8,
        },
        "latest": {
            "date": latest_date,
            "value": score,
            "score": score,
            "state": classify(score),
            "bias": "diagnostic_only",
        },
        "state": {
            "level": classify(score),
            "score": score,
            "cflow_composite_score": cflow["score"],
            "iv_vector_score": vector_score,
            "active_vector_count": len(active_vectors),
            "target_vector_count": 8,
            "coverage": f"{len(active_vectors)}/8",
            "interpretation": (
                "C•FLOW is soft/benign with full IV[t] routing coverage."
                if score < 25
                else "C•FLOW is under watch with full IV[t] routing coverage."
                if score < 50
                else "C•FLOW is in stress with full IV[t] routing coverage."
                if score < 75
                else "C•FLOW is constrained with full IV[t] routing coverage."
            ),
        },
        "iv_vector": iv_vector,
        "attribution": {
            "cflow_composite": {
                "date": cflow["date"],
                "score": cflow["score"],
                "state": cflow["state"],
                "weight": 0.60,
            },
            "iv_vector_contribution": {
                "date": str(iv_latest["date"]),
                "score": vector_score,
                "state": classify(vector_score),
                "weight": 0.40,
                "active_vector_count": len(active_vectors),
            },
            "econ_composite": {
                "date": econ["date"],
                "score": econ["score"],
                "state": econ["state"],
            },
            "capital_composite": {
                "date": capital["date"],
                "score": capital["score"],
                "state": capital["state"],
            },
            "financial_transmission_composite": {
                "date": financial["date"],
                "score": financial["score"],
                "state": financial["state"],
            },
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
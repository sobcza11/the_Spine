import json
from pathlib import Path
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "state_engine": ROOT / "data/serving/cflow/cflow_state_engine_serving.json",
    "econ": ROOT / "data/serving/cflow/econ_composite_serving.json",
    "capital": ROOT / "data/serving/cflow/capital_composite_serving.json",
}

OUTPUT = ROOT / "data/serving/cflow/cflow_regime_engine_serving.json"


REGIME_LABELS = [
    "Expansion",
    "Softening",
    "Watch",
    "Stress",
    "Fragmentation",
    "Constraint",
]


def load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def latest_score(payload: dict, fallback: float = 0.0) -> float:
    latest = payload.get("latest", {})
    for key in ("score", "value", "composite_score"):
        if key in latest:
            return float(latest[key])
    return fallback


def extract_vectors(payload: dict) -> dict:
    source = payload.get("iv_vector")

    if not isinstance(source, dict):
        raise ValueError("Missing top-level iv_vector block in state engine.")

    vectors = {}

    for key in ("P", "F", "L", "D", "M", "X", "C", "S"):
        item = source.get(key)

        if not isinstance(item, dict):
            raise ValueError(f"Missing IV[t] vector: {key}")

        vectors[key] = float(item.get("score", 0.0))

    return vectors

def classify_regime(vectors: dict, econ_score: float, capital_score: float, cflow_score: float) -> tuple[str, list[str]]:
    P = vectors["P"]
    D = vectors["D"]
    X = vectors["X"]
    C = vectors["C"]
    S = vectors["S"]

    reasons = []

    # Hard deterministic routing rules.
    if S >= 70:
        reasons.append("Systemicity is elevated enough to classify current regime as Constraint.")
        return "Constraint", reasons

    if D >= 65 and C <= 40:
        reasons.append("Dispersion is high while coherence is weak.")
        return "Fragmentation", reasons

    if P >= 60 and X >= 60 and D >= 60:
        reasons.append("Pressure, cross-asset stress, and dispersion are simultaneously elevated.")
        return "Stress", reasons

    if econ_score <= 40 and capital_score >= 55:
        reasons.append("Economic composite is weak while capital composite is elevated.")
        return "Watch", reasons

    # Weighted score classification.
    weighted_score = (
        0.20 * P
        + 0.15 * vectors["F"]
        + 0.10 * vectors["L"]
        + 0.15 * D
        + 0.10 * vectors["M"]
        + 0.15 * X
        + 0.05 * C
        + 0.10 * S
    )

    if weighted_score < 30:
        regime = "Expansion"
    elif weighted_score < 45:
        regime = "Softening"
    elif weighted_score < 60:
        regime = "Watch"
    elif weighted_score < 75:
        regime = "Stress"
    else:
        regime = "Constraint"

    reasons.append(f"Weighted current-state score classified regime as {regime}.")
    return regime, reasons


def build_regime_engine() -> dict:
    state_engine = load_json(INPUTS["state_engine"])
    econ = load_json(INPUTS["econ"])
    capital = load_json(INPUTS["capital"])

    vectors = extract_vectors(state_engine)
    cflow_score = latest_score(state_engine)
    econ_score = latest_score(econ)
    capital_score = latest_score(capital)

    regime, reasons = classify_regime(
        vectors=vectors,
        econ_score=econ_score,
        capital_score=capital_score,
        cflow_score=cflow_score,
    )

    now = datetime.now(timezone.utc).isoformat()

    return {
        "metric": "C•FLOW Regime Engine",
        "category": "C•FLOW",
        "sub_category": "Regime Classification",
        "source": "the_Spine deterministic state engine",
        "frequency": "Derived from latest available C•FLOW serving inputs",
        "meta": {
            "generated_at": now,
            "model_type": "deterministic_classification",
            "forecasting": False,
            "probability": False,
            "trade_recommendation": False,
            "allowed_labels": REGIME_LABELS,
            "inputs": {k: str(v) for k, v in INPUTS.items()},
        },
        "latest": {
            "regime": regime,
            "cflow_score": cflow_score,
            "econ_score": econ_score,
            "capital_score": capital_score,
            "vectors": vectors,
            "rules_applied": reasons,
        },
        "attribution": {
            "primary_drivers": sorted(
                vectors.items(),
                key=lambda x: x[1],
                reverse=True,
            )[:3],
            "primary_offsets": sorted(
                vectors.items(),
                key=lambda x: x[1],
            )[:3],
            "diagnostic_note": "Current regime classification only. No forecast, probability, or trade recommendation.",
        },
    }


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    payload = build_regime_engine()

    with OUTPUT.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()


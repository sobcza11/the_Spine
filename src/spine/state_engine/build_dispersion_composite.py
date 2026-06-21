from pathlib import Path
import json
import statistics

ROOT = Path(__file__).resolve().parents[3]

LABOR = ROOT / "data/serving/cflow/labor_composite_serving.json"
INFLATION = ROOT / "data/serving/cflow/inflation_composite_serving.json"
ENERGY = ROOT / "data/serving/cflow/energy_composite_serving.json"
TRANSPORT = ROOT / "data/serving/cflow/transport_transmission_composite_serving.json"
ECON = ROOT / "data/serving/cflow/econ_composite_serving.json"

OUTPUT = ROOT / "data/serving/cflow/dispersion_composite_serving.json"


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
        return "dispersion_low"
    if score < 50:
        return "dispersion_watch"
    if score < 75:
        return "dispersion_stress"
    return "dispersion_extreme"


def main():
    labor = load_latest(LABOR)
    inflation = load_latest(INFLATION)
    energy = load_latest(ENERGY)
    transport = load_latest(TRANSPORT)
    econ = load_latest(ECON)

    component_scores = [
        labor["score"],
        inflation["score"],
        energy["score"],
        transport["score"],
    ]

    # Cross-system spread: high when subcomponents disagree sharply.
    dispersion_raw = statistics.pstdev(component_scores)

    # Scale 0-100 conservatively; cap extreme instability.
    dispersion_score = round(min(dispersion_raw * 2.0, 100), 2)

    latest_date = max(
        labor["date"],
        inflation["date"],
        energy["date"],
        transport["date"],
        econ["date"],
    )

    out = {
        "metric": "Dispersion Composite",
        "category": "IV[t]",
        "sub_category": "Structural Fragility",
        "source": "the_Spine | C•FLOW dispersion engine",
        "frequency": "Mixed",
        "meta": {
            "name": "Dispersion Composite",
            "forecasting": "prohibited",
            "phase": "8R",
            "method": "cross_component_standard_deviation_v1",
            "iv_target": "D",
        },
        "latest": {
            "date": latest_date,
            "value": dispersion_score,
            "score": dispersion_score,
            "state": classify(dispersion_score),
            "bias": "diagnostic_only",
        },
        "attribution": {
            "labor_composite": {
                "date": labor["date"],
                "score": labor["score"],
                "state": labor["state"],
                "role": "dispersion_component",
            },
            "inflation_composite": {
                "date": inflation["date"],
                "score": inflation["score"],
                "state": inflation["state"],
                "role": "dispersion_component",
            },
            "energy_composite": {
                "date": energy["date"],
                "score": energy["score"],
                "state": energy["state"],
                "role": "dispersion_component",
            },
            "transport_transmission": {
                "date": transport["date"],
                "score": transport["score"],
                "state": transport["state"],
                "role": "dispersion_component",
            },
            "econ_composite": {
                "date": econ["date"],
                "score": econ["score"],
                "state": econ["state"],
                "role": "context_anchor",
            },
            "dispersion_math": {
                "component_count": len(component_scores),
                "raw_population_stdev": round(dispersion_raw, 4),
                "scaling": "score = min(population_stdev * 2.0, 100)",
            },
        },
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
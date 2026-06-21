from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "activity": {
        "path": ROOT / "data/serving/c_flow/weekly_economic_index_serving.json",
        "weight": 0.20,
    },
    "labor": {
        "path": ROOT / "data/serving/cflow/labor_composite_serving.json",
        "weight": 0.20,
    },
    "inflation": {
        "path": ROOT / "data/serving/cflow/inflation_composite_serving.json",
        "weight": 0.20,
    },
    "energy": {
        "path": ROOT / "data/serving/cflow/energy_composite_serving.json",
        "weight": 0.20,
    },
    "transport": {
        "path": ROOT / "data/serving/cflow/transport_transmission_composite_serving.json",
        "weight": 0.20,
    },
}

OUTPUT = ROOT / "data/serving/cflow/econ_composite_serving.json"


def load_latest(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    latest = payload["latest"]

    return {
        "date": str(latest.get("date", "")),
        "score": float(latest.get("score", latest.get("value", 0))),
        "state": str(latest.get("state", "unknown")),
    }


def classify(score: float) -> str:
    if score < 25:
        return "econ_soft"
    if score < 50:
        return "econ_watch"
    if score < 75:
        return "econ_expansion"
    return "econ_pressure"


def main():
    attribution = {}
    weighted_sum = 0
    used_weight = 0
    dates = []

    for key, cfg in INPUTS.items():
        path = cfg["path"]
        weight = cfg["weight"]

        if not path.exists():
            attribution[key] = {
                "available": False,
                "path": str(path),
                "weight": weight,
            }
            continue

        latest = load_latest(path)

        weighted_sum += latest["score"] * weight
        used_weight += weight

        if latest["date"]:
            dates.append(latest["date"])

        attribution[key] = {
            "available": True,
            "date": latest["date"],
            "score": latest["score"],
            "state": latest["state"],
            "weight": weight,
        }

    if used_weight == 0:
        raise RuntimeError("No Econ inputs available.")

    score = round(weighted_sum / used_weight, 2)

    out = {
        "metric": "Econ Composite",
        "category": "Physical Economy",
        "sub_category": "Composite",
        "source": "the_Spine | C•FLOW Econ",
        "frequency": "Mixed",
        "meta": {
            "name": "Econ Composite",
            "forecasting": "prohibited",
            "phase": "8L",
            "method": "available_weighted_component_score_v1",
            "used_weight": round(used_weight, 4),
        },
        "latest": {
            "date": max(dates) if dates else None,
            "value": score,
            "score": score,
            "state": classify(score),
            "bias": "diagnostic_only",
        },
        "attribution": attribution,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()

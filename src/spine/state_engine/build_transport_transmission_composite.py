from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "cass_freight_shipments": {
        "path": ROOT / "data/serving/cflow/cass_freight_shipments_serving.json",
        "weight": 0.20,
    },
    "freight_transportation_services": {
        "path": ROOT / "data/serving/cflow/freight_transportation_services_serving.json",
        "weight": 0.20,
    },
    "rail_freight_carloads": {
        "path": ROOT / "data/serving/cflow/rail_freight_carloads_serving.json",
        "weight": 0.20,
    },
    "rail_freight_intermodal": {
        "path": ROOT / "data/serving/cflow/rail_freight_intermodal_serving.json",
        "weight": 0.15,
    },
    "container_shipping_index": {
        "path": ROOT / "data/serving/cflow/container_shipping_index_serving.json",
        "weight": 0.15,
    },
    "baltic_dry_proxy": {
        "path": ROOT / "data/serving/cflow/baltic_dry_proxy_serving.json",
        "weight": 0.10,
    },
}

OUTPUT = ROOT / "data/serving/cflow/transport_transmission_composite_serving.json"


def load_latest(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    latest = payload.get("latest", {})
    return {
        "date": str(latest.get("date", "")),
        "score": float(latest.get("score", latest.get("value", 0))),
        "state": str(latest.get("state", "unknown")),
    }


def classify(score: float) -> str:
    if score < 25:
        return "freight_soft"
    if score < 50:
        return "freight_watch"
    if score < 75:
        return "freight_normal"
    return "freight_stressed"


def main():
    rows = {}
    weighted_sum = 0.0
    used_weight = 0.0
    latest_dates = []

    for key, cfg in INPUTS.items():
        path = cfg["path"]
        weight = float(cfg["weight"])

        if not path.exists():
            rows[key] = {
                "available": False,
                "path": str(path),
                "weight": weight,
            }
            continue

        latest = load_latest(path)
        score = latest["score"]

        weighted_sum += score * weight
        used_weight += weight

        if latest["date"]:
            latest_dates.append(latest["date"])

        rows[key] = {
            "available": True,
            "date": latest["date"],
            "score": score,
            "state": latest["state"],
            "weight": weight,
        }

    if used_weight == 0:
        raise RuntimeError("No transport transmission inputs available.")

    score = round(weighted_sum / used_weight, 2)
    latest_date = max(latest_dates) if latest_dates else None

    out = {
        "metric": "Transport Transmission Composite",
        "category": "Physical Economy",
        "sub_category": "Logistics / Freight",
        "source": "the_Spine | C•FLOW transport transmission",
        "frequency": "Mixed",
        "meta": {
            "name": "Transport Transmission Composite",
            "ft_gmi_role": "Transport Transmission",
            "forecasting": "prohibited",
            "phase": "8K",
            "method": "available_weighted_component_score_v1",
            "used_weight": round(used_weight, 4),
        },
        "latest": {
            "date": latest_date,
            "value": score,
            "score": score,
            "state": classify(score),
            "bias": "diagnostic_only",
        },
        "attribution": rows,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"OK | wrote {OUTPUT}")


if __name__ == "__main__":
    main()


    
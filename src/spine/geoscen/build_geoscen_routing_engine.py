import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

INPUTS = {
    "shipping":
        ROOT / "data/serving/geoscen/geoscen_shipping_routes_serving.json",

    "energy":
        ROOT / "data/serving/geoscen/geoscen_energy_geography_serving.json",

    "minerals":
        ROOT / "data/serving/geoscen/geoscen_critical_minerals_serving.json",

    "chokepoints":
        ROOT / "data/serving/geoscen/geoscen_chokepoints_serving.json",
}

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_routing_engine_serving.json"
)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_route(payload, key):
    return (
        payload.get("routing", {}).get(key)
        or 0.0
    )


def main():

    shipping = load_json(INPUTS["shipping"])
    energy = load_json(INPUTS["energy"])
    minerals = load_json(INPUTS["minerals"])
    chokepoints = load_json(INPUTS["chokepoints"])

    P = round(
        (
            safe_route(shipping, "P")
            + safe_route(energy, "P")
            + safe_route(minerals, "P")
        ),
        2
    )

    D = round(
        safe_route(minerals, "D"),
        2
    )

    X = round(
        (
            safe_route(shipping, "X")
            + safe_route(energy, "X")
            + safe_route(chokepoints, "X")
        ),
        2
    )

    S = round(
        (
            safe_route(energy, "S")
            + safe_route(minerals, "S")
            + safe_route(chokepoints, "S")
        ),
        2
    )

    payload = {
        "metric": "GeoScen Routing Engine",
        "category": "GeoScen",
        "sub_category": "IV[t] Routing",
        "source": "the_Spine",
        "frequency": "Mixed",
        "meta": {
            "generated_at":
                datetime.now(timezone.utc).isoformat(),
            "forecasting": False,
            "prediction": False,
            "trade_recommendation": False
        },
        "latest": {
            "P": P,
            "D": D,
            "X": X,
            "S": S
        },
        "routing": {
            "Pressure": P,
            "Dispersion": D,
            "Cross_Asset_Transmission": X,
            "Systemicity": S
        },
        "inputs": {
            "shipping": INPUTS["shipping"].name,
            "energy": INPUTS["energy"].name,
            "minerals": INPUTS["minerals"].name,
            "chokepoints": INPUTS["chokepoints"].name
        }
    }

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            payload,
            f,
            indent=2
        )

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

    
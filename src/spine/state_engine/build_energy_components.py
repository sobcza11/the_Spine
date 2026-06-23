from pathlib import Path
import json


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "data" / "serving" / "cflow"


def write_payload(filename, payload):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / filename

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, allow_nan=False)

    print(f"OK | built {path}")


def main():
    diesel_payload = {
        "meta": {
            "name": "Diesel Demand",
            "source": "the_Spine | C•FLOW energy component",
            "method": "manual_component_stub_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Energy Transmission",
            "cflow_domain": "Physical Economy",
            "cflow_subsystem": "Energy Demand",
            "frequency": "Weekly",
        },
        "latest": {
            "date": "2026-06-18",
            "value": 5.0,
            "score": 5.0,
            "state": "diesel_demand_expanding",
            "bias": "Real Economy Fuel Demand Firm",
        },
    }

    distillate_payload = {
        "meta": {
            "name": "Distillate Inventories",
            "source": "the_Spine | C•FLOW energy component",
            "method": "manual_component_stub_v1",
            "forecasting": "prohibited",
            "ft_gmi_role": "Energy Transmission",
            "cflow_domain": "Physical Economy",
            "cflow_subsystem": "Energy Demand",
            "frequency": "Weekly",
        },
        "latest": {
            "date": "2026-06-18",
            "value": 3.8,
            "score": 3.8,
            "state": "distillate_inventory_tightness",
            "bias": "Inventory Constraint Moderate",
        },
    }

    write_payload("diesel_demand_serving.json", diesel_payload)
    write_payload("distillate_inventories_serving.json", distillate_payload)


if __name__ == "__main__":
    main()

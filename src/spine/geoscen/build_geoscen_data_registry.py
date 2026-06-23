import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_data_registry_serving.json"
)

DATASETS = [
    {
        "name": "Baltic Dry Index",
        "source_file": "baltic_dry_proxy_serving.json",
        "domain": "Shipping Routes"
    },
    {
        "name": "Container Shipping Index",
        "source_file": "container_shipping_index_serving.json",
        "domain": "Shipping Routes"
    },
    {
        "name": "Energy Composite",
        "source_file": "energy_composite_serving.json",
        "domain": "Energy Geography"
    },
    {
        "name": "Transport Transmission",
        "source_file": "transport_transmission_composite_serving.json",
        "domain": "Shipping Routes"
    },
    {
        "name": "COT Positioning",
        "source_file": "cot_positioning_serving.json",
        "domain": "Critical Minerals"
    }
]

payload = {
    "metric": "GeoScen Data Registry",
    "category": "GeoScen",
    "sub_category": "Data Registry",
    "source": "the_Spine",
    "frequency": "On Build",
    "meta": {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "dataset_count": len(DATASETS)
    },
    "latest": {
        "status": "Registry Active"
    },
    "datasets": DATASETS
}

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Wrote {OUTPUT}")


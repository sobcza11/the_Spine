import json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]

OUTPUT = (
    ROOT
    / "data"
    / "serving"
    / "geoscen"
    / "geoscen_domain_registry_serving.json"
)

DOMAINS = [
    {
        "id": "energy",
        "name": "Energy Geography",
        "future_builder": "build_geoscen_energy_geography.py",
        "routes_to": ["P", "X", "S"]
    },
    {
        "id": "shipping",
        "name": "Shipping Routes",
        "future_builder": "build_geoscen_shipping_routes.py",
        "routes_to": ["P", "X"]
    },
    {
        "id": "minerals",
        "name": "Critical Minerals",
        "future_builder": "build_geoscen_critical_minerals.py",
        "routes_to": ["P", "D", "S"]
    },
    {
        "id": "food",
        "name": "Food Supply",
        "future_builder": "build_geoscen_food_supply.py",
        "routes_to": ["P", "D"]
    },
    {
        "id": "industry",
        "name": "Industrial Capacity",
        "future_builder": "build_geoscen_industrial_capacity.py",
        "routes_to": ["P", "D"]
    },
    {
        "id": "chokepoints",
        "name": "Strategic Chokepoints",
        "future_builder": "build_geoscen_chokepoints.py",
        "routes_to": ["X", "S"]
    },
    {
        "id": "regional_stress",
        "name": "Regional Stress",
        "future_builder": "build_geoscen_regional_stress.py",
        "routes_to": ["P", "D", "X", "S"]
    }
]

payload = {
    "metric": "GeoScen Domain Registry",
    "category": "GeoScen",
    "sub_category": "Registry",
    "source": "the_Spine",
    "frequency": "On Build",
    "meta": {
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "forecasting": False,
        "prediction": False,
        "trade_recommendation": False,
        "domain_count": len(DOMAINS)
    },
    "latest": {
        "status": "Registry Active",
        "domain_count": len(DOMAINS)
    },
    "domains": DOMAINS
}

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Wrote {OUTPUT}")


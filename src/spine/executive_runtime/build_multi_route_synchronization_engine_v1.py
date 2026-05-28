from pathlib import Path
from datetime import datetime, UTC
import json


def build_multi_route_synchronization_engine_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    routes_path = site / "config" / "geoscen_routes.json"
    routes = json.loads(routes_path.read_text(encoding="utf-8"))["routes"]

    payload = {
        "component": "multi_route_synchronization_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "route_count": len(routes),
        "routes": [
            {
                "route": r["route"],
                "label": r["label"],
                "component_count": len(r.get("components", []))
            }
            for r in routes
        ],
        "status": "multi_route_synchronization_ready"
    }

    out = site / "executive_runtime" / "multi_route_synchronization_engine_v1.json"

    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Multi-Route Synchronization Engine complete")
    print("Routes:", payload["route_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_multi_route_synchronization_engine_v1()

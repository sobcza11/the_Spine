from pathlib import Path
from datetime import datetime, UTC
import json
import shutil


ROUTES = [
    "executive",
    "finstate",
    "cflow",
    "fx",
    "rates",
    "equities_industry",
    "equities_sector"
]


def build_route_deployment_bundles_v1():

    root = Path.cwd()
    site = root / "_offline_site"
    bundles = site / "cloudflare" / "bundles" / "routes"

    if bundles.exists():
        shutil.rmtree(bundles)

    bundles.mkdir(parents=True, exist_ok=True)

    rows = []

    for route in ROUTES:

        route_dir = bundles / route
        route_dir.mkdir(parents=True, exist_ok=True)

        route_src = site / "routes" / route / "route.json"

        if route_src.exists():
            shutil.copy2(route_src, route_dir / "route.json")

        rows.append({
            "route": route,
            "bundle_path": str(route_dir.relative_to(site)).replace("\\", "/"),
            "route_json_exists": route_src.exists()
        })

    payload = {
        "component": "route_deployment_bundles_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "route_count": len(rows),
        "routes": rows,
        "status": "route_deployment_bundles_ready"
    }

    out = site / "cloudflare" / "manifests" / "route_deployment_bundles_v1.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Route Deployment Bundles complete")
    print("Routes:", payload["route_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_route_deployment_bundles_v1()

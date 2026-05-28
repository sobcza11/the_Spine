from pathlib import Path
from datetime import datetime, UTC
import json


def validate_runtime_routes_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    routes_dir = site / "routes"

    rows = []

    for route_dir in routes_dir.iterdir():

        if not route_dir.is_dir():
            continue

        route_json = route_dir / "route.json"

        rows.append({
            "route": route_dir.name,
            "exists": route_json.exists(),
            "size_bytes": (
                route_json.stat().st_size
                if route_json.exists()
                else 0
            )
        })

    missing = [r for r in rows if not r["exists"]]

    summary = {
        "component": "runtime_route_validation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "route_count": len(rows),
        "missing_count": len(missing),
        "missing": missing,
        "status": (
            "runtime_routes_valid"
            if not missing
            else "runtime_routes_invalid"
        )
    }

    out = (
        site
        / "deploy_manifest"
        / "runtime_route_validation_v1.json"
    )

    out.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8"
    )

    print("Runtime Route Validation complete")
    print("Routes:", summary["route_count"])
    print("Missing:", summary["missing_count"])
    print("Status:", summary["status"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    validate_runtime_routes_v1()

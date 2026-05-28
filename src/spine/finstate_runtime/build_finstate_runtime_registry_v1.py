from pathlib import Path
from datetime import datetime, UTC
import json


def build_finstate_runtime_registry_v1():

    root = Path.cwd()

    runtime = root / "_offline_site" / "finstate_runtime"

    runtime.mkdir(parents=True, exist_ok=True)

    registry = {
        "component": "finstate_runtime_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "domains": [
            "survivability",
            "fragility",
            "deterioration",
            "systemicity",
            "recursive_pressure",
            "antifragility"
        ],
        "integration_targets": [
            "GeoScen",
            "I2",
            "C_FLOW",
            "RATES",
            "FX"
        ],
        "status": "finstate_runtime_registry_ready"
    }

    out = runtime / "finstate_runtime_registry_v1.json"

    out.write_text(
        json.dumps(registry, indent=2),
        encoding="utf-8"
    )

    print("FINSTATE Runtime Registry complete")
    print("Domains:", len(registry["domains"]))
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_finstate_runtime_registry_v1()

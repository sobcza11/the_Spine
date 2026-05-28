from pathlib import Path
from datetime import datetime, UTC
import json


def build_finstate_executive_synthesis_v1():

    root = Path.cwd()

    payload_dir = root / "_offline_site" / "finstate_payloads"

    files = sorted(payload_dir.glob("*.json"))

    components = []

    for f in files:

        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue

        components.append({
            "file": f.name,
            "component": data.get("component", f.stem),
            "status": data.get("status", "unknown")
        })

    synthesis = {
        "component": "finstate_executive_synthesis_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "component_count": len(components),
        "components": components,
        "executive_state": "finstate_survivability_runtime_operational",
        "interpretation": (
            "FINSTATE is operating as the corporate survivability cognition layer, "
            "linking I2 quarterly deterioration, recursive fragility, contagion, "
            "persistence, anti-fragility, systemicity, and GeoScen coupling."
        ),
        "status": "finstate_executive_synthesis_ready"
    }

    out = payload_dir / "finstate_executive_synthesis_v1.json"

    out.write_text(
        json.dumps(synthesis, indent=2),
        encoding="utf-8"
    )

    print("FINSTATE Executive Synthesis complete")
    print("Components:", synthesis["component_count"])
    print("State:", synthesis["executive_state"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_finstate_executive_synthesis_v1()

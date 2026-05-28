from pathlib import Path
from datetime import datetime, UTC
import json


def build_runtime_governance_layer_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    governance = {
        "component": "runtime_governance_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "rules": {
            "registry_first": True,
            "ai_last": True,
            "offline_first": True,
            "source_provenance_required": True,
            "runtime_validation_required": True,
            "executive_outputs_required": True
        },
        "governed_domains": [
            "GeoScen",
            "FINSTATE",
            "C_FLOW",
            "FX",
            "RATES",
            "Equities",
            "Executive"
        ],
        "status": "runtime_governance_layer_ready"
    }

    out = site / "executive_runtime" / "runtime_governance_layer_v1.json"
    out.write_text(json.dumps(governance, indent=2), encoding="utf-8")

    print("Runtime Governance Layer complete")
    print("Domains:", len(governance["governed_domains"]))
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_runtime_governance_layer_v1()

from pathlib import Path
from datetime import datetime, UTC
import json


def validate_survivability_governance_v1():

    root = Path.cwd()

    site = root / "_offline_site"

    required = [
        "finstate_runtime/finstate_runtime_registry_v1.json",
        "finstate_payloads/i2_survivability_loader_v1.json",
        "finstate_payloads/quarterly_deterioration_propagation_v1.json",
        "finstate_payloads/corporate_contagion_engine_v1.json",
        "finstate_payloads/survivability_memory_system_v1.json",
        "finstate_payloads/recursive_pressure_conditioning_v1.json",
        "finstate_payloads/sector_survivability_ranking_v1.json",
        "finstate_payloads/recursive_survivability_graph_v1.json",
        "finstate_payloads/finstate_executive_synthesis_v1.json",
        "payloads/manifests/quarterly_survivability_manifest_v1.json"
    ]

    rows = []

    for rel in required:

        path = site / rel

        rows.append({
            "path": rel,
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else 0
        })

    missing = [r for r in rows if not r["exists"]]

    summary = {
        "component": "survivability_governance_validation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "required_count": len(required),
        "existing_count": len(required) - len(missing),
        "missing_count": len(missing),
        "missing": missing,
        "governance": {
            "finstate_route_ready": len(missing) == 0,
            "manifest_registered": (site / "payloads/manifests/quarterly_survivability_manifest_v1.json").exists(),
            "runtime_payloads_present": len(missing) == 0,
            "offline_ready": len(missing) == 0
        },
        "status": "survivability_governance_valid" if not missing else "survivability_governance_invalid"
    }

    out = site / "deploy_manifest" / "survivability_governance_validation_v1.json"

    out.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8"
    )

    print("Survivability Governance Validation complete")
    print("Required:", summary["required_count"])
    print("Existing:", summary["existing_count"])
    print("Missing:", summary["missing_count"])
    print("Status:", summary["status"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    validate_survivability_governance_v1()

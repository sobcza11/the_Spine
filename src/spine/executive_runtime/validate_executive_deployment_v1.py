from pathlib import Path
from datetime import datetime, UTC
import json


def validate_executive_deployment_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    required = [
        "executive_runtime/executive_runtime_control_center_v1.json",
        "executive_runtime/executive_regime_transition_engine_v1.json",
        "executive_runtime/multi_route_synchronization_engine_v1.json",
        "executive_runtime/global_systemic_pressure_engine_v1.json",
        "executive_runtime/macro_corporate_commodity_fusion_v1.json",
        "executive_runtime/dynamic_regime_probability_engine_v1.json",
        "executive_runtime/runtime_governance_layer_v1.json",
        "core/executive_runtime_loader.js",
        "components/dashboard/executive_runtime_dashboard.js"
    ]

    rows = []

    for rel in required:
        p = site / rel
        rows.append({
            "path": rel,
            "exists": p.exists(),
            "size_bytes": p.stat().st_size if p.exists() else 0
        })

    missing = [r for r in rows if not r["exists"]]

    summary = {
        "component": "executive_deployment_validation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "required_count": len(required),
        "existing_count": len(required) - len(missing),
        "missing_count": len(missing),
        "missing": missing,
        "status": "executive_deployment_valid" if not missing else "executive_deployment_invalid"
    }

    out = site / "deploy_manifest" / "executive_deployment_validation_v1.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Executive Deployment Validation complete")
    print("Required:", summary["required_count"])
    print("Existing:", summary["existing_count"])
    print("Missing:", summary["missing_count"])
    print("Status:", summary["status"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    validate_executive_deployment_v1()

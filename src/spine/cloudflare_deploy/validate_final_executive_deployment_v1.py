from pathlib import Path
from datetime import datetime, UTC
import json


def validate_final_executive_deployment_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    required = [
        "cloudflare/manifests/cloudflare_offline_deployment_architecture_v1.json",
        "cloudflare/manifests/secure_runtime_export_v1.json",
        "cloudflare/manifests/static_payload_compilation_v1.json",
        "cloudflare/manifests/payload_compression_engine_v1.json",
        "cloudflare/manifests/route_deployment_bundles_v1.json",
        "cloudflare/manifests/cloudflare_pages_structure_v1.json",
        "cloudflare/manifests/runtime_cdn_asset_registry_v1.json",
        "cloudflare/manifests/payload_integrity_verification_v1.json",
        "cloudflare/manifests/runtime_backup_system_v1.json",
        "cloudflare/manifests/executive_export_snapshot_v1.json",
        "cloudflare/manifests/deployment_promotion_pipeline_v1.json",
        "cloudflare/manifests/offline_runtime_release_governance_v1.json",
        "cloudflare/manifests/production_runtime_manifest_v1.json",
        "cloudflare/manifests/operational_security_layer_v1.json"
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
        "component": "final_executive_deployment_validation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "required_count": len(required),
        "existing_count": len(required) - len(missing),
        "missing_count": len(missing),
        "missing": missing,
        "status": "final_executive_deployment_valid" if not missing else "final_executive_deployment_invalid"
    }

    out = site / "cloudflare" / "manifests" / "final_executive_deployment_validation_v1.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Final Executive Deployment Validation complete")
    print("Required:", summary["required_count"])
    print("Existing:", summary["existing_count"])
    print("Missing:", summary["missing_count"])
    print("Status:", summary["status"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    validate_final_executive_deployment_v1()

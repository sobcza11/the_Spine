from pathlib import Path
from datetime import datetime, UTC
import json


def build_executive_production_readiness_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    validation_path = site / "cloudflare" / "manifests" / "final_executive_deployment_validation_v1.json"
    package_path = site / "cloudflare" / "manifests" / "cloudflare_production_package_v1.json"
    manifest_path = site / "cloudflare" / "manifests" / "production_runtime_manifest_v1.json"

    validation = json.loads(validation_path.read_text(encoding="utf-8")) if validation_path.exists() else {}
    package = json.loads(package_path.read_text(encoding="utf-8")) if package_path.exists() else {}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}

    ready = (
        validation.get("status") == "final_executive_deployment_valid"
        and package.get("status") == "cloudflare_production_package_ready"
        and manifest.get("entrypoint_exists") is True
    )

    summary = {
        "component": "executive_production_readiness_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "validation_status": validation.get("status"),
        "package_status": package.get("status"),
        "entrypoint_exists": manifest.get("entrypoint_exists"),
        "file_count": manifest.get("file_count"),
        "production_ready": ready,
        "status": "executive_production_ready" if ready else "executive_production_not_ready"
    }

    out_json = site / "cloudflare" / "manifests" / "executive_production_readiness_v1.json"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    out_md = site / "cloudflare" / "manifests" / "executive_production_readiness_v1.md"

    md = []
    md.append("# Executive Production Readiness")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append(f"Status: {summary['status']}")
    md.append(f"Production Ready: {summary['production_ready']}")
    md.append(f"Validation: {summary['validation_status']}")
    md.append(f"Package: {summary['package_status']}")
    md.append(f"Entrypoint Exists: {summary['entrypoint_exists']}")
    md.append(f"File Count: {summary['file_count']}")

    out_md.write_text("\n".join(md), encoding="utf-8")

    print("Executive Production Readiness complete")
    print("Ready:", summary["production_ready"])
    print("Status:", summary["status"])
    print("OUTPUT:", out_md)


if __name__ == "__main__":
    build_executive_production_readiness_v1()

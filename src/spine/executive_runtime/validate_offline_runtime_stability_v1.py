from pathlib import Path
from datetime import datetime, UTC
import json


def validate_offline_runtime_stability_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    html = site / "index.html"

    js_files = list((site / "core").glob("*.js"))
    component_js = list((site / "components").rglob("*.js"))
    json_files = list(site.rglob("*.json"))

    checks = {
        "index_exists": html.exists(),
        "core_js_count": len(js_files),
        "component_js_count": len(component_js),
        "json_payload_count": len(json_files),
        "has_routes": (site / "config" / "geoscen_routes.json").exists(),
        "has_runtime_manifest": (site / "config" / "runtime_manifest_v1.json").exists(),
        "has_finstate_summary": (site / "deploy_manifest" / "finstate_operationalization_summary_v1.json").exists(),
        "has_executive_validation": (site / "deploy_manifest" / "executive_deployment_validation_v1.json").exists()
    }

    passed = (
        checks["index_exists"]
        and checks["core_js_count"] > 0
        and checks["component_js_count"] > 0
        and checks["json_payload_count"] > 0
        and checks["has_routes"]
        and checks["has_runtime_manifest"]
    )

    summary = {
        "component": "offline_runtime_stability_validation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "checks": checks,
        "status": "offline_runtime_stable" if passed else "offline_runtime_unstable"
    }

    out = site / "deploy_manifest" / "offline_runtime_stability_validation_v1.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Offline Runtime Stability Validation complete")
    print("Core JS:", checks["core_js_count"])
    print("Component JS:", checks["component_js_count"])
    print("JSON payloads:", checks["json_payload_count"])
    print("Status:", summary["status"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    validate_offline_runtime_stability_v1()

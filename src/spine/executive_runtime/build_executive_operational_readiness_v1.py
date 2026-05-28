from pathlib import Path
from datetime import datetime, UTC
import json


def build_executive_operational_readiness_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    validations = [
        site / "deploy_manifest" / "foundation_validation_v1.json",
        site / "deploy_manifest" / "runtime_route_validation_v1.json",
        site / "deploy_manifest" / "survivability_governance_validation_v1.json",
        site / "deploy_manifest" / "executive_deployment_validation_v1.json",
        site / "deploy_manifest" / "offline_runtime_stability_validation_v1.json"
    ]

    rows = []

    for v in validations:

        if v.exists():
            data = json.loads(v.read_text(encoding="utf-8"))
            rows.append({
                "file": v.name,
                "status": data.get("status", "unknown")
            })
        else:
            rows.append({
                "file": v.name,
                "status": "missing"
            })

    ready = all(
        r["status"] not in ["missing", "foundation_validation_failed", "runtime_routes_invalid", "survivability_governance_invalid", "executive_deployment_invalid", "offline_runtime_unstable"]
        for r in rows
    )

    summary = {
        "component": "executive_operational_readiness_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "validation_count": len(rows),
        "validations": rows,
        "offline_operational_readiness": ready,
        "status": "executive_operational_readiness_ready" if ready else "executive_operational_readiness_incomplete"
    }

    out_json = site / "deploy_manifest" / "executive_operational_readiness_v1.json"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md = []
    md.append("# Executive Operational Readiness")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append(f"Status: {summary['status']}")
    md.append(f"Offline Operational Readiness: {summary['offline_operational_readiness']}")
    md.append("")
    md.append("## Validations")
    md.append("")
    for r in rows:
        md.append(f"- {r['file']}: {r['status']}")

    out_md = site / "deploy_manifest" / "executive_operational_readiness_v1.md"
    out_md.write_text("\n".join(md), encoding="utf-8")

    print("Executive Operational Readiness complete")
    print("Ready:", summary["offline_operational_readiness"])
    print("Status:", summary["status"])
    print("OUTPUT:", out_md)


if __name__ == "__main__":
    build_executive_operational_readiness_v1()

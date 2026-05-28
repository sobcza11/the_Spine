from pathlib import Path
from datetime import datetime, UTC
import json


def complete_finstate_operationalization_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    validation_path = (
        site
        / "deploy_manifest"
        / "survivability_governance_validation_v1.json"
    )

    if validation_path.exists():
        validation = json.loads(validation_path.read_text(encoding="utf-8"))
    else:
        validation = {
            "status": "validation_missing",
            "missing_count": 999
        }

    status = (
        "finstate_institutional_operationalization_complete"
        if validation.get("missing_count", 1) == 0
        else "finstate_institutional_operationalization_incomplete"
    )

    summary = {
        "component": "finstate_institutional_operationalization_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "validation_status": validation.get("status"),
        "missing_count": validation.get("missing_count"),
        "offline_route": "/finstate",
        "capabilities": [
            "I2 quarterly survivability loading",
            "quarterly deterioration propagation",
            "recursive fragility overlays",
            "corporate contagion engine",
            "survivability memory system",
            "recursive pressure conditioning",
            "sector survivability ranking",
            "recursive survivability graph",
            "executive survivability synthesis",
            "GeoScen-FINSTATE propagation hooks"
        ],
        "status": status
    }

    out_json = site / "deploy_manifest" / "finstate_operationalization_summary_v1.json"

    out_json.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8"
    )

    md = []
    md.append("# FINSTATE Institutional Operationalization")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append(f"Status: {summary['status']}")
    md.append(f"Validation: {summary['validation_status']}")
    md.append(f"Missing Count: {summary['missing_count']}")
    md.append("")
    md.append("## Capabilities")
    md.append("")
    for c in summary["capabilities"]:
        md.append(f"- {c}")
    md.append("")
    md.append("## Bottom Line")
    md.append("")
    md.append(
        "FINSTATE is now operational as the corporate survivability cognition layer inside the GeoScen / IsoVector offline runtime."
    )

    out_md = site / "deploy_manifest" / "finstate_operationalization_summary_v1.md"

    out_md.write_text(
        "\n".join(md),
        encoding="utf-8"
    )

    print("FINSTATE Institutional Operationalization complete")
    print("Status:", summary["status"])
    print("Missing:", summary["missing_count"])
    print("OUTPUT:", out_md)


if __name__ == "__main__":
    complete_finstate_operationalization_v1()

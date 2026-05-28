from pathlib import Path
from datetime import datetime, UTC
import json


def validate_foundation_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    required = [
        "index.html",
        "config/runtime_contract_v1.json",
        "config/geoscen_routes.json",
        "config/route_metadata.json",
        "config/offline_asset_registry.json",
        "config/runtime_manifest_v1.json",
        "config/oc_segment_registry_v1.json",
        "core/runtime_loader.js",
        "core/navigation.js",
        "core/route_state_engine.js",
        "core/payload_validator.js",
        "core/runtime_guard.js",
        "core/runtime_hydration.js",
        "css/global.css",
        "css/theme.css",
        "components/cards/metric_card.js",
        "components/cards/metric_cards.js",
        "components/narratives/narrative_block.js",
        "components/narratives/narrative_engine.js",
        "components/tables/table_engine.js",
        "components/heatmaps/heatmap_engine.js",
        "components/graphs/graph_engine.js",
        "components/executive_header/executive_header.js",
        "deploy_manifest/static_offline_build_structure_v1.json",
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
        "component": "offline_site_foundation_validation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "required_count": len(required),
        "existing_count": len(required) - len(missing),
        "missing_count": len(missing),
        "missing": missing,
        "status": "foundation_validation_passed" if not missing else "foundation_validation_failed"
    }

    out_json = site / "deploy_manifest" / "foundation_validation_v1.json"

    out_json.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8"
    )

    md = []
    md.append("# GeoScen Offline Site Foundation Validation")
    md.append("")
    md.append(f"Generated: {summary['generated_at_utc']}")
    md.append("")
    md.append(f"Required: {summary['required_count']}")
    md.append(f"Existing: {summary['existing_count']}")
    md.append(f"Missing: {summary['missing_count']}")
    md.append(f"Status: {summary['status']}")
    md.append("")

    if missing:
        md.append("## Missing")
        for m in missing:
            md.append(f"- {m['path']}")
    else:
        md.append("## Result")
        md.append("Phase 1 foundation is complete. Offline site structure is Cloudflare-ready.")

    out_md = site / "deploy_manifest" / "foundation_validation_v1.md"

    out_md.write_text(
        "\n".join(md),
        encoding="utf-8"
    )

    print("Offline Site Foundation Validation complete")
    print("Required:", summary["required_count"])
    print("Existing:", summary["existing_count"])
    print("Missing:", summary["missing_count"])
    print("Status:", summary["status"])
    print("OUTPUT:", out_md)


if __name__ == "__main__":
    validate_foundation_v1()

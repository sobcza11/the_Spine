from pathlib import Path
from datetime import datetime, UTC
import json
import shutil


def build_executive_export_snapshot_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    snapshot_dir = site / "cloudflare" / "executive_snapshot"
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)

    snapshot_dir.mkdir(parents=True, exist_ok=True)

    include = [
        site / "deploy_manifest" / "executive_cognitive_runtime_completion_v1.md",
        site / "deploy_manifest" / "executive_operational_readiness_v1.md",
        site / "deploy_manifest" / "finstate_operationalization_summary_v1.md",
        site / "cloudflare" / "manifests" / "runtime_cdn_asset_registry_v1.json",
        site / "cloudflare" / "manifests" / "payload_integrity_verification_v1.json"
    ]

    copied = []

    for p in include:
        if p.exists():
            target = snapshot_dir / p.name
            shutil.copy2(p, target)
            copied.append(p.name)

    summary = {
        "component": "executive_export_snapshot_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "snapshot_path": str(snapshot_dir),
        "copied_count": len(copied),
        "copied": copied,
        "status": "executive_export_snapshot_ready"
    }

    out = site / "cloudflare" / "manifests" / "executive_export_snapshot_v1.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Executive Export Snapshot complete")
    print("Copied:", summary["copied_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_executive_export_snapshot_v1()

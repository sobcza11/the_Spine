from pathlib import Path
from datetime import datetime, UTC
import json
import shutil


def build_deployment_promotion_pipeline_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    pages_export = site / "cloudflare" / "pages_export"
    production = site / "cloudflare" / "production_candidate"

    if not pages_export.exists():
        raise FileNotFoundError(f"Missing pages export: {pages_export}")

    if production.exists():
        shutil.rmtree(production)

    shutil.copytree(pages_export, production)

    payload = {
        "component": "deployment_promotion_pipeline_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source": str(pages_export),
        "production_candidate": str(production),
        "file_count": len([p for p in production.rglob("*") if p.is_file()]),
        "status": "deployment_promotion_candidate_ready"
    }

    out = site / "cloudflare" / "manifests" / "deployment_promotion_pipeline_v1.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Deployment Promotion Pipeline complete")
    print("Files:", payload["file_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_deployment_promotion_pipeline_v1()

from pathlib import Path
from datetime import datetime, UTC
import json


def build_runtime_cdn_asset_registry_v1():

    root = Path.cwd()
    site = root / "_offline_site"
    export = site / "cloudflare" / "pages_export"

    if not export.exists():
        raise FileNotFoundError(
            f"Missing Cloudflare Pages export: {export}"
        )

    assets = []

    for p in export.rglob("*"):

        if p.is_file():

            rel = str(p.relative_to(export)).replace("\\", "/")

            assets.append({
                "path": rel,
                "size_bytes": p.stat().st_size,
                "cdn_path": f"/{rel}"
            })

    registry = {
        "component": "runtime_cdn_asset_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "asset_count": len(assets),
        "total_bytes": sum(a["size_bytes"] for a in assets),
        "assets": assets,
        "status": "runtime_cdn_asset_registry_ready"
    }

    out = site / "cloudflare" / "manifests" / "runtime_cdn_asset_registry_v1.json"
    out.write_text(json.dumps(registry, indent=2), encoding="utf-8")

    print("Runtime CDN Asset Registry complete")
    print("Assets:", registry["asset_count"])
    print("Bytes:", registry["total_bytes"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_runtime_cdn_asset_registry_v1()

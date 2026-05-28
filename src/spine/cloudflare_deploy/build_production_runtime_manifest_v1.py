from pathlib import Path
from datetime import datetime, UTC
import json


def build_production_runtime_manifest_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    production = site / "cloudflare" / "production_candidate"

    if not production.exists():
        raise FileNotFoundError(f"Missing production candidate: {production}")

    files = []

    for p in production.rglob("*"):
        if p.is_file():
            files.append({
                "path": str(p.relative_to(production)).replace("\\", "/"),
                "size_bytes": p.stat().st_size
            })

    manifest = {
        "component": "production_runtime_manifest_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "production_candidate": str(production),
        "file_count": len(files),
        "total_bytes": sum(f["size_bytes"] for f in files),
        "entrypoint_exists": (production / "index.html").exists(),
        "files": files,
        "status": "production_runtime_manifest_ready"
    }

    out = site / "cloudflare" / "manifests" / "production_runtime_manifest_v1.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Production Runtime Manifest complete")
    print("Files:", manifest["file_count"])
    print("Bytes:", manifest["total_bytes"])
    print("Entrypoint:", manifest["entrypoint_exists"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_production_runtime_manifest_v1()

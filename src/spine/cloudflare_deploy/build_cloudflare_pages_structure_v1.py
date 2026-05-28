from pathlib import Path
from datetime import datetime, UTC
import json
import shutil


def build_cloudflare_pages_structure_v1():

    root = Path.cwd()
    site = root / "_offline_site"
    export = site / "cloudflare" / "pages_export"

    if export.exists():
        shutil.rmtree(export)

    export.mkdir(parents=True, exist_ok=True)

    source = site / "cloudflare" / "secure_runtime_export"

    if not source.exists():
        raise FileNotFoundError(
            f"Missing secure runtime export: {source}"
        )

    for p in source.rglob("*"):

        if p.is_file():

            rel = p.relative_to(source)
            target = export / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, target)

    manifest = {
        "component": "cloudflare_pages_structure_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "pages_export": str(export),
        "file_count": len([p for p in export.rglob("*") if p.is_file()]),
        "entrypoint_exists": (export / "index.html").exists(),
        "status": "cloudflare_pages_structure_ready"
    }

    out = site / "cloudflare" / "manifests" / "cloudflare_pages_structure_v1.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Cloudflare Pages Structure complete")
    print("Files:", manifest["file_count"])
    print("Entrypoint:", manifest["entrypoint_exists"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_cloudflare_pages_structure_v1()

from pathlib import Path
from datetime import datetime, UTC
import json


def build_runtime_manifest_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    runtime_dirs = [
        site / "geoscen_runtime",
        site / "langroid_runtime",
        site / "oc_ai_components",
        site / "oc_segments",
        site / "runtime",
        site / "config",
        site / "core",
        site / "components",
    ]

    assets = []

    for runtime_dir in runtime_dirs:

        if not runtime_dir.exists():
            continue

        for path in runtime_dir.rglob("*"):

            if path.is_file():

                assets.append({
                    "path": str(path.relative_to(site)).replace("\\", "/"),
                    "suffix": path.suffix,
                    "size_bytes": path.stat().st_size,
                    "modified_utc": datetime.fromtimestamp(
                        path.stat().st_mtime,
                        UTC
                    ).isoformat()
                })

    manifest = {
        "component": "offline_site_runtime_manifest_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "asset_count": len(assets),
        "assets": assets,
        "status": "runtime_manifest_complete"
    }

    out = site / "config" / "runtime_manifest_v1.json"

    out.parent.mkdir(parents=True, exist_ok=True)

    out.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8"
    )

    print("Offline Site Runtime Manifest complete")
    print("Assets:", manifest["asset_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_runtime_manifest_v1()

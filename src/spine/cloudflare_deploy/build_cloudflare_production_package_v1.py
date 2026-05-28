from pathlib import Path
from datetime import datetime, UTC
import json
import shutil
import zipfile


def build_cloudflare_production_package_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    production = site / "cloudflare" / "production_candidate"

    if not production.exists():
        raise FileNotFoundError(f"Missing production candidate: {production}")

    package_dir = site / "cloudflare" / "package"
    package_dir.mkdir(parents=True, exist_ok=True)

    zip_path = package_dir / "isovector_geoscen_cloudflare_pages_package_v1.zip"

    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:

        for p in production.rglob("*"):

            if p.is_file():
                z.write(p, p.relative_to(production))

    manifest = {
        "component": "cloudflare_production_package_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "package_path": str(zip_path),
        "package_size_bytes": zip_path.stat().st_size,
        "source": str(production),
        "status": "cloudflare_production_package_ready"
    }

    out = site / "cloudflare" / "manifests" / "cloudflare_production_package_v1.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Cloudflare Production Package complete")
    print("Package:", zip_path)
    print("Bytes:", manifest["package_size_bytes"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_cloudflare_production_package_v1()

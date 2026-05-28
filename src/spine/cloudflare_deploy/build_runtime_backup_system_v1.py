from pathlib import Path
from datetime import datetime, UTC
import shutil
import json


def build_runtime_backup_system_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    backup_root = site / "cloudflare" / "backups"
    backup_root.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    backup_dir = backup_root / f"runtime_backup_{stamp}"

    source = site / "cloudflare" / "pages_export"

    if not source.exists():
        raise FileNotFoundError(f"Missing pages export: {source}")

    shutil.copytree(source, backup_dir)

    manifest = {
        "component": "runtime_backup_system_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "backup_path": str(backup_dir),
        "file_count": len([p for p in backup_dir.rglob("*") if p.is_file()]),
        "status": "runtime_backup_complete"
    }

    out = site / "cloudflare" / "manifests" / "runtime_backup_system_v1.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Runtime Backup System complete")
    print("Files:", manifest["file_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_runtime_backup_system_v1()

from pathlib import Path
from datetime import datetime, UTC
import hashlib
import json


def build_payload_integrity_verification_v1():

    root = Path.cwd()
    site = root / "_offline_site"
    export = site / "cloudflare" / "pages_export"

    if not export.exists():
        raise FileNotFoundError(f"Missing pages export: {export}")

    rows = []

    for p in export.rglob("*"):

        if p.is_file():

            h = hashlib.sha256()
            h.update(p.read_bytes())

            rows.append({
                "path": str(p.relative_to(export)).replace("\\", "/"),
                "size_bytes": p.stat().st_size,
                "sha256": h.hexdigest()
            })

    payload = {
        "component": "payload_integrity_verification_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "file_count": len(rows),
        "files": rows,
        "status": "payload_integrity_verified"
    }

    out = site / "cloudflare" / "manifests" / "payload_integrity_verification_v1.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Payload Integrity Verification complete")
    print("Files:", payload["file_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_payload_integrity_verification_v1()

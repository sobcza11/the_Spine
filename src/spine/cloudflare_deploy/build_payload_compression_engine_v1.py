from pathlib import Path
from datetime import datetime, UTC
import json
import gzip
import shutil


def build_payload_compression_engine_v1():

    root = Path.cwd()
    site = root / "_offline_site"
    compressed = site / "cloudflare" / "bundles" / "compressed_payloads"

    if compressed.exists():
        shutil.rmtree(compressed)

    compressed.mkdir(parents=True, exist_ok=True)

    gz_files = []

    for p in site.rglob("*.json"):

        rel_text = str(p.relative_to(site)).replace("\\", "/")

        if rel_text.startswith("cloudflare/"):
            continue

        out = compressed / f"{rel_text}.gz"
        out.parent.mkdir(parents=True, exist_ok=True)

        with open(p, "rb") as f_in:
            with gzip.open(out, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        gz_files.append({
            "source": rel_text,
            "compressed": str(out.relative_to(site)).replace("\\", "/"),
            "source_bytes": p.stat().st_size,
            "compressed_bytes": out.stat().st_size
        })

    payload = {
        "component": "payload_compression_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "compressed_file_count": len(gz_files),
        "files": gz_files,
        "status": "payload_compression_ready"
    }

    manifest = site / "cloudflare" / "manifests" / "payload_compression_engine_v1.json"
    manifest.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Payload Compression Engine complete")
    print("Compressed:", payload["compressed_file_count"])
    print("OUTPUT:", manifest)


if __name__ == "__main__":
    build_payload_compression_engine_v1()

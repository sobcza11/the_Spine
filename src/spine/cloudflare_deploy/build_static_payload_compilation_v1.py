from pathlib import Path
from datetime import datetime, UTC
import json


def build_static_payload_compilation_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    json_files = []

    for p in site.rglob("*.json"):

        if "cloudflare/secure_runtime_export" in str(p).replace("\\", "/"):
            continue

        json_files.append({
            "path": str(p.relative_to(site)).replace("\\", "/"),
            "size_bytes": p.stat().st_size
        })

    payload = {
        "component": "static_payload_compilation_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "json_payload_count": len(json_files),
        "total_json_bytes": sum(x["size_bytes"] for x in json_files),
        "payloads": json_files,
        "status": "static_payload_compilation_ready"
    }

    out = site / "cloudflare" / "manifests" / "static_payload_compilation_v1.json"

    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Static Payload Compilation complete")
    print("JSON payloads:", payload["json_payload_count"])
    print("Bytes:", payload["total_json_bytes"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_static_payload_compilation_v1()

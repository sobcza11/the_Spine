from pathlib import Path
from datetime import datetime, UTC
import json


def normalize_payload(path):

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "path": str(path),
            "status": "json_read_failed",
            "error": str(exc)
        }

    normalized = {
        "component": data.get("component", path.stem),
        "version": data.get("version", "v1"),
        "built_at_utc": (
            data.get("built_at_utc")
            or data.get("timestamp_utc")
            or data.get("generated_at_utc")
            or datetime.now(UTC).isoformat()
        ),
        "status": data.get("status", "normalized_runtime_available"),
        "source_file": path.name,
        "payload": data
    }

    out = path.parent / f"{path.stem}_normalized.json"

    out.write_text(
        json.dumps(normalized, indent=2),
        encoding="utf-8"
    )

    return {
        "path": str(path),
        "output": str(out),
        "status": "normalized"
    }


def normalize_geoscen_runtime_v1():

    root = Path.cwd()
    runtime = root / "_offline_site" / "geoscen_runtime"

    results = []

    for path in runtime.glob("*.json"):

        if path.name.endswith("_normalized.json"):
            continue

        results.append(normalize_payload(path))

    summary = {
        "component": "geoscen_runtime_normalization_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "normalized_count": sum(1 for r in results if r["status"] == "normalized"),
        "results": results,
        "status": "geoscen_runtime_normalization_complete"
    }

    out = root / "_offline_site" / "config" / "geoscen_runtime_normalization_summary_v1.json"

    out.write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8"
    )

    print("GeoScen Runtime Normalization complete")
    print("Normalized:", summary["normalized_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    normalize_geoscen_runtime_v1()

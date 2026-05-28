from pathlib import Path
from datetime import datetime, UTC
import json


def build_oc_segment_registry_v1():

    root = Path.cwd()
    site = root / "_offline_site"

    segment_root = site / "oc_segments"
    component_root = site / "oc_ai_components"

    segments = []

    if segment_root.exists():

        for folder in segment_root.iterdir():

            if folder.is_dir():

                segments.append({
                    "name": folder.name,
                    "type": "oc_segment",
                    "path": str(folder.relative_to(site)).replace("\\", "/"),
                    "file_count": len([p for p in folder.rglob("*") if p.is_file()])
                })

    if component_root.exists():

        for folder in component_root.iterdir():

            if folder.is_dir():

                segments.append({
                    "name": folder.name,
                    "type": "oc_ai_component",
                    "path": str(folder.relative_to(site)).replace("\\", "/"),
                    "file_count": len([p for p in folder.rglob("*") if p.is_file()])
                })

    registry = {
        "component": "oc_segment_registry_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "segment_count": len(segments),
        "segments": segments,
        "status": "oc_segment_registry_complete"
    }

    out = site / "config" / "oc_segment_registry_v1.json"

    out.write_text(
        json.dumps(registry, indent=2),
        encoding="utf-8"
    )

    print("OC Segment Registry complete")
    print("Segments:", registry["segment_count"])
    print("OUTPUT:", out)


if __name__ == "__main__":
    build_oc_segment_registry_v1()

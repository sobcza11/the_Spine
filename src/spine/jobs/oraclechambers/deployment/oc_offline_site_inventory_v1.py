from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json


REPO_ROOT = Path(__file__).resolve().parents[5]

SEGMENT_ROOT = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
)

REQUIRED_FILES = [
    "index.html",
    "styles.css",
    "app.js",
    "manifest.json",
    "payloads/oc_local_site_hydration_v1.json",
]


def inspect_site(site_dir: Path) -> dict[str, Any]:
    missing_files = [
        file_name for file_name in REQUIRED_FILES
        if not (site_dir / file_name).exists()
    ]

    manifest_path = site_dir / "manifest.json"

    manifest: dict[str, Any] = {}

    if manifest_path.exists():
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

    return {
        "site_name": site_dir.name,
        "path": str(site_dir),
        "offline_site_ready": len(missing_files) == 0,
        "missing_files": missing_files,
        "domain": manifest.get("domain"),
        "phase": manifest.get("phase"),
        "online_transition_allowed": manifest.get(
            "online_transition_allowed",
            False,
        ),
    }


def build_offline_site_inventory_v1() -> dict[str, Any]:
    if not SEGMENT_ROOT.exists():
        return {
            "artifact": "oc_offline_site_inventory_v1",
            "run_ts": datetime.now(timezone.utc).isoformat(),
            "inventory_ready": False,
            "reason": f"Missing segment root: {SEGMENT_ROOT}",
            "sites": [],
        }

    sites = [
        inspect_site(site_dir)
        for site_dir in sorted(SEGMENT_ROOT.iterdir())
        if site_dir.is_dir()
    ]

    return {
        "artifact": "oc_offline_site_inventory_v1",
        "layer": "OracleChambers Offline Site Inventory",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "inventory_ready": all(site["offline_site_ready"] for site in sites),
        "site_count": len(sites),
        "online_transition_allowed": False,
        "sites": sites,
    }


if __name__ == "__main__":
    print(json.dumps(build_offline_site_inventory_v1(), indent=2))

    
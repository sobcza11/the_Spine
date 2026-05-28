from pathlib import Path
import json
from datetime import datetime, timezone


REPO_ROOT = Path(__file__).resolve().parents[5]
VIEWER_ROOT = REPO_ROOT / "_offline_site" / "oc_ai_components"

VIEWERS = [
    ("fx", "fx_ai_components_v1.json"),
    ("rates", "rates_ai_components_v1.json"),
    ("c_flow", "cflow_ai_components_v1.json"),
    ("equities_sector", "equities_sector_ai_components_v1.json"),
    ("equities_industry", "equities_industry_ai_components_v1.json"),
]

REQUIRED_SITE_FILES = [
    "index.html",
    "styles.css",
    "app.js",
    "manifest.json",
]


def validate_viewer(site: str, payload_name: str) -> dict:
    site_dir = VIEWER_ROOT / site
    failures = []

    if not site_dir.exists():
        failures.append("missing_site_dir")

    for file_name in REQUIRED_SITE_FILES:
        if not (site_dir / file_name).exists():
            failures.append(f"missing_file:{file_name}")

    payload_path = site_dir / "payloads" / payload_name
    if not payload_path.exists():
        failures.append(f"missing_payload:{payload_name}")
    else:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        if "zt" not in payload:
            failures.append("missing_zt")
        if "rbl" not in payload:
            failures.append("missing_rbl")
        if payload.get("zt", {}).get("online_transition_allowed"):
            failures.append("zt_online_gate_open")
        if payload.get("rbl", {}).get("online_transition_allowed"):
            failures.append("rbl_online_gate_open")

    manifest_path = site_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not manifest.get("viewer_ready"):
            failures.append("viewer_not_ready")
        if manifest.get("online_transition_allowed"):
            failures.append("viewer_online_gate_open")

    return {
        "site": site,
        "payload": payload_name,
        "passed": len(failures) == 0,
        "failures": failures,
    }


def main() -> None:
    results = [
        validate_viewer(site, payload)
        for site, payload in VIEWERS
    ]

    failures = [
        result for result in results
        if not result["passed"]
    ]

    index_exists = (VIEWER_ROOT / "index.html").exists()

    if not index_exists:
        failures.append({
            "site": "index",
            "payload": None,
            "passed": False,
            "failures": ["missing_index"],
        })

    output = {
        "artifact": "test_ai_component_viewer_sites_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "passed": len(failures) == 0,
        "viewer_count": len(results),
        "failed_viewers": len(failures),
        "results": results,
    }

    print(json.dumps(output, indent=2))

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
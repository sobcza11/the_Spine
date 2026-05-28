from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

SEGMENT_ROOT = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
)

PLANES = [
    {
        "site_name": "oc-fx-local",
        "domain": "FX",
    },
    {
        "site_name": "oc-rates-local",
        "domain": "RATES",
    },
    {
        "site_name": "oc-cflow-local",
        "domain": "C_FLOW",
    },
    {
        "site_name": "oc-equities-index-local",
        "domain": "EQUITIES_INDEX",
    },
    {
        "site_name": "oc-equities-sector-local",
        "domain": "EQUITIES_SECTOR",
    },
]

REQUIRED_FILES = [
    "index.html",
    "styles.css",
    "app.js",
    "manifest.json",
    "payloads/oc_local_site_hydration_v1.json",
]


def validate_plane(plane: dict) -> dict:

    site_name = plane["site_name"]
    expected_domain = plane["domain"]

    site_dir = SEGMENT_ROOT / site_name

    failures = []

    if not site_dir.exists():
        failures.append("missing_site_directory")

    for file_name in REQUIRED_FILES:

        file_path = site_dir / file_name

        if not file_path.exists():
            failures.append(f"missing_file:{file_name}")

    manifest_path = site_dir / "manifest.json"

    manifest = {}

    if manifest_path.exists():

        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

        if manifest.get("site_name") != site_name:
            failures.append("invalid_site_name")

        if manifest.get("domain") != expected_domain:
            failures.append("invalid_domain")

        if manifest.get("offline_site_ready") is not True:
            failures.append("offline_site_not_ready")

        if manifest.get("online_transition_allowed") is True:
            failures.append("online_transition_should_be_blocked")

    payload_path = (
        site_dir
        / "payloads"
        / "oc_local_site_hydration_v1.json"
    )

    payload_checks = {
        "headline_exists": False,
        "narrative_exists": False,
        "historical_memory_exists": False,
    }

    if payload_path.exists():

        payload = json.loads(
            payload_path.read_text(encoding="utf-8")
        )

        site_payload = payload.get("site_payload", {})

        if "headline" in site_payload:
            payload_checks["headline_exists"] = True

        if "narrative" in site_payload:
            payload_checks["narrative_exists"] = True

        if "historical_memory" in site_payload:
            payload_checks["historical_memory_exists"] = True

    return {
        "site_name": site_name,
        "domain": expected_domain,
        "passed": len(failures) == 0,
        "failures": failures,
        "payload_checks": payload_checks,
    }


def main() -> None:

    results = []

    for plane in PLANES:
        results.append(
            validate_plane(plane)
        )

    total_planes = len(results)

    passed_planes = len([
        r for r in results
        if r["passed"]
    ])

    failed_planes = len([
        r for r in results
        if not r["passed"]
    ])

    aggregate_status = {
        "artifact": "test_phase_b_domain_plane_aggregator_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "total_planes": total_planes,
        "passed_planes": passed_planes,
        "failed_planes": failed_planes,
        "all_planes_ready": failed_planes == 0,
        "results": results,
    }

    print(
        json.dumps(
            aggregate_status,
            indent=2,
        )
    )

    if failed_planes > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
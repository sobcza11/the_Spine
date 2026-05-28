from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()
SEGMENT_ROOT = REPO_ROOT / "_offline_site" / "oc_segments"

PLANES = [
    ("oc-liquidity-stress-map-local", "LIQUIDITY_STRESS_MAP"),
    ("oc-cb-divergence-map-local", "CB_DIVERGENCE_MAP"),
    ("oc-executive-dashboard-local", "EXECUTIVE_DASHBOARD"),
]

REQUIRED_FILES = [
    "index.html",
    "styles.css",
    "app.js",
    "manifest.json",
    "payloads/oc_local_site_hydration_v1.json",
]


def validate(site_name: str, domain: str) -> dict:
    site_dir = SEGMENT_ROOT / site_name
    failures = []

    if not site_dir.exists():
        failures.append("missing_site")

    for file_name in REQUIRED_FILES:
        if not (site_dir / file_name).exists():
            failures.append(f"missing_file:{file_name}")

    manifest_path = site_dir / "manifest.json"

    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        if manifest.get("site_name") != site_name:
            failures.append("invalid_site_name")

        if manifest.get("domain") != domain:
            failures.append("invalid_domain")

        if manifest.get("offline_site_ready") is not True:
            failures.append("offline_not_ready")

        if manifest.get("online_transition_allowed") is True:
            failures.append("online_gate_open_should_be_closed")

    return {
        "site_name": site_name,
        "domain": domain,
        "passed": len(failures) == 0,
        "failures": failures,
    }


def main() -> None:
    results = [validate(site_name, domain) for site_name, domain in PLANES]
    failed = len([r for r in results if not r["passed"]])

    result = {
        "artifact": "test_phase_d_visual_planes_4_6_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "total_planes": len(results),
        "failed_planes": failed,
        "all_planes_ready": failed == 0,
        "results": results,
    }

    print(json.dumps(result, indent=2))

    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
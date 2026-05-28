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
        "site_name":"oc-narrative-drift-local",
        "domain":"NARRATIVE_DRIFT",
    },
    {
        "site_name":"oc-fedspeak-local",
        "domain":"FEDSPEAK",
    },
    {
        "site_name":"oc-earnings-tone-local",
        "domain":"EARNINGS_TONE",
    },
]

REQUIRED_FILES = [
    "index.html",
    "styles.css",
    "app.js",
    "manifest.json",
    "payloads/oc_local_site_hydration_v1.json",
]


def validate_plane(plane:dict)->dict:

    site_name = plane["site_name"]
    domain = plane["domain"]

    site_dir = SEGMENT_ROOT / site_name

    failures = []

    if not site_dir.exists():
        failures.append("missing_site")

    for file_name in REQUIRED_FILES:

        if not (site_dir / file_name).exists():
            failures.append(f"missing_file:{file_name}")

    manifest_path = site_dir / "manifest.json"

    if manifest_path.exists():

        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

        if manifest.get("domain") != domain:
            failures.append("invalid_domain")

        if manifest.get("offline_site_ready") is not True:
            failures.append("offline_not_ready")

    return {
        "site_name":site_name,
        "domain":domain,
        "passed":len(failures)==0,
        "failures":failures,
    }


def main():

    results = []

    for plane in PLANES:
        results.append(validate_plane(plane))

    failures = len([
        r for r in results
        if not r["passed"]
    ])

    result = {
        "artifact":"test_phase_c_nlp_planes_v1",
        "run_ts":datetime.now(timezone.utc).isoformat(),
        "total_planes":len(results),
        "failed_planes":failures,
        "all_planes_ready":failures==0,
        "results":results,
    }

    print(json.dumps(result, indent=2))

    if failures > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
    
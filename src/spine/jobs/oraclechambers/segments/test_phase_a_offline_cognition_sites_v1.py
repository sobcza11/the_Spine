from pathlib import Path
import json

REPO_ROOT = Path.cwd()
SITE_ROOT = REPO_ROOT / "_offline_site" / "oc_segments"

REQUIRED_SITES = [
    "oc-rbl-local",
    "oc-contradiction-local",
    "oc-historical-memory-local",
    "oc-final-metric-local",
    "oc-attention-routing-local",
    "oc-governance-local",
]

REQUIRED_FILES = [
    "index.html",
    "styles.css",
    "app.js",
    "manifest.json",
    "payloads/oc_local_site_hydration_v1.json",
]


def main() -> None:
    failures = []

    for site in REQUIRED_SITES:
        site_dir = SITE_ROOT / site

        if not site_dir.exists():
            failures.append(f"missing_site: {site}")
            continue

        for file_name in REQUIRED_FILES:
            path = site_dir / file_name
            if not path.exists():
                failures.append(f"missing_file: {site}/{file_name}")

        manifest_path = site_dir / "manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            if not manifest.get("offline_site_ready"):
                failures.append(f"not_ready: {site}")

            if manifest.get("online_transition_allowed"):
                failures.append(f"online_gate_open_should_be_closed: {site}")

    result = {
        "artifact": "test_phase_a_offline_cognition_sites_v1",
        "passed": len(failures) == 0,
        "site_count": len(REQUIRED_SITES),
        "failures": failures,
    }

    print(json.dumps(result, indent=2))

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
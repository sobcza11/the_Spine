from pathlib import Path
import json

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-equities-sector-local"
)

REQUIRED_FILES = [
    "index.html",
    "styles.css",
    "app.js",
    "manifest.json",
    "payloads/oc_local_site_hydration_v1.json",
]


def main() -> None:
    failures = []

    if not SITE_DIR.exists():
        failures.append("missing_equities_sector_plane")

    for file_name in REQUIRED_FILES:
        if not (SITE_DIR / file_name).exists():
            failures.append(f"missing_file: {file_name}")

    manifest_path = SITE_DIR / "manifest.json"

    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        if manifest.get("site_name") != "oc-equities-sector-local":
            failures.append("invalid_site_name")

        if manifest.get("domain") != "EQUITIES_SECTOR":
            failures.append("invalid_domain")

        if manifest.get("online_transition_allowed"):
            failures.append("online_gate_open_should_be_closed")

    result = {
        "artifact": "test_phase_b_equities_sector_plane_v1",
        "passed": len(failures) == 0,
        "failures": failures,
    }

    print(json.dumps(result, indent=2))

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
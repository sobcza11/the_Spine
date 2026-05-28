from pathlib import Path
import json

REPO_ROOT = Path.cwd()

SITE_DIR = (
    REPO_ROOT
    / "_offline_site"
    / "oc_segments"
    / "oc-fx-local"
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
        failures.append("missing_fx_plane")

    for file_name in REQUIRED_FILES:
        path = SITE_DIR / file_name

        if not path.exists():
            failures.append(f"missing_file: {file_name}")

    manifest_path = SITE_DIR / "manifest.json"

    if manifest_path.exists():

        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

        if manifest.get("site_name") != "oc-fx-local":
            failures.append("invalid_site_name")

        if manifest.get("domain") != "FX":
            failures.append("invalid_domain")

    result = {
        "artifact": "test_phase_b_fx_plane_v1",
        "passed": len(failures) == 0,
        "failures": failures,
    }

    print(json.dumps(result, indent=2))

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

    
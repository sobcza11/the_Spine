from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]

SCRIPTS = [
    "src/spine/jobs/energy/build_wti_price_json.py",
    "src/spine/jobs/energy/build_wti_inventory_json.py",
    "src/spine/jobs/energy/upload_wti_json_to_r2.py",
]


def run_script(script_path: str) -> None:
    full_path = REPO_ROOT / script_path
    if not full_path.exists():
        raise FileNotFoundError(f"Script not found: {full_path}")

    print(f"\n=== RUNNING: {full_path} ===")
    result = subprocess.run(
        [sys.executable, str(full_path)],
        cwd=str(REPO_ROOT),
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Script failed with exit code {result.returncode}: {full_path}")


def main() -> None:
    print(f"REPO_ROOT: {REPO_ROOT}")

    for script in SCRIPTS:
        run_script(script)

    print("\nWTI JSON pipeline complete.")


if __name__ == "__main__":
    main()
    
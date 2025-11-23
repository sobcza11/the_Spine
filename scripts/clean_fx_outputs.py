"""
scripts/clean_fx_outputs.py

Utility script to clean locally generated FX Stream parquet outputs so they
can be safely regenerated & re-uploaded to R2.

It ONLY deletes known FX-related output files if they exist.

Usage (from repo root):
    python scripts/clean_fx_outputs.py
    python scripts/clean_fx_outputs.py --dry-run
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List


# List of FX-related parquet outputs we are comfortable deleting locally.
FX_OUTPUT_PATHS: List[str] = [
    # FX raw & canonical (possible locations)
    "spine_us/us_fx_fed_raw.parquet",
    "spine_us/us_fx_spot_canonical.parquet",

    # 10Y yield leaf & 10Y spread leaf
    "spine_us/us_fx_10y_yields.parquet",
    "spine_us/us_fx_10y_spreads.parquet",

    # In case any outputs were written into src/data/R2
    "src/data/R2/us_fx_fed_raw.parquet",
    "src/data/R2/us_fx_spot_canonical.parquet",
    "src/data/R2/us_fx_10y_yields.parquet",
    "src/data/R2/us_fx_10y_spreads.parquet",
]


def clean_fx_outputs(repo_root: Path, dry_run: bool = False) -> None:
    print(f"[CLEAN-FX] Repo root: {repo_root}")
    print(f"[CLEAN-FX] Dry run: {dry_run}")
    print("--------------------------------------------------")

    for rel_path in FX_OUTPUT_PATHS:
        full_path = repo_root / rel_path
        if full_path.exists():
            if dry_run:
                print(f"[CLEAN-FX] WOULD delete: {full_path}")
            else:
                try:
                    full_path.unlink()
                    print(f"[CLEAN-FX] Deleted: {full_path}")
                except Exception as exc:
                    print(f"[CLEAN-FX] ERROR deleting {full_path}: {exc}")
        else:
            print(f"[CLEAN-FX] Not found (ok): {full_path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean locally generated FX Stream parquet outputs."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting files.",
    )
    args = parser.parse_args()

    # Assume this script sits in <repo_root>/scripts/
    repo_root = Path(__file__).resolve().parents[1]

    clean_fx_outputs(repo_root=repo_root, dry_run=args.dry_run)
    print("--------------------------------------------------")
    print("[CLEAN-FX] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


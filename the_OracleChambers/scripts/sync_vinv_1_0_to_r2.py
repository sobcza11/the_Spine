#!/usr/bin/env python
"""
Sync VinV_1.0 core outputs into R2 under the_Spine layout.

Assumptions:
- You run this from the project root: the_Spine/the_OracleChambers
- VinV_1.0 has already been built via: python scripts/run_vinv_1_0_demo.py
- R2 env vars are set in your shell:
    SPINE_R2_ACCOUNT_ID
    SPINE_R2_ACCESS_KEY_ID
    SPINE_R2_SECRET_ACCESS_KEY
    SPINE_R2_BUCKET
    SPINE_R2_BASE_PREFIX  (e.g. "the_Spine/")
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Tuple

# --- Make src/ importable ----------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.append(str(SRC))

from r2_client import (  # type: ignore[import]
    get_r2_client,
    R2_BUCKET,
    make_spine_key,
)
from vinv.vinv_monthly import get_vinv_root  # type: ignore[import]


def iter_vinv_artifacts(vinv_root: Path) -> Iterable[Tuple[Path, str]]:
    """
    Yield (local_path, spine_rel_key) pairs for VinV_1.0 core artifacts.
    spine_rel_key is relative to SPINE_R2_BASE_PREFIX.
    """
    files = [
        (
            vinv_root / "vinv_monthly_panel.parquet",
            "vinv/1_0/monthly_panels/vinv_monthly_panel.parquet",
        ),
        (
            vinv_root / "vinv_portfolio_monthly.parquet",
            "vinv/1_0/portfolio/vinv_portfolio_monthly.parquet",
        ),
        (
            vinv_root / "vinv_benchmarks_monthly.parquet",
            "vinv/1_0/benchmarks/vinv_benchmarks_monthly.parquet",
        ),
    ]
    for local_path, rel_key in files:
        yield local_path, rel_key


def upload_to_r2(local_path: Path, rel_key: str) -> None:
    """
    Upload a single file to R2 under the the_Spine/ prefix.

    rel_key is the logical key under the Spine base prefix, e.g.:
        "vinv/1_0/portfolio/vinv_portfolio_monthly.parquet"
    """
    from botocore.exceptions import ClientError  # type: ignore[import]

    s3 = get_r2_client()
    key = make_spine_key(rel_key)

    print(f"[UPLOAD] {local_path} -> s3://{R2_BUCKET}/{key}")

    if not local_path.exists():
        print(f"  [SKIP] local file not found: {local_path}")
        return

    try:
        with local_path.open("rb") as f:
            s3.put_object(
                Bucket=R2_BUCKET,
                Key=key,
                Body=f,
                ContentType="application/octet-stream",
            )
        print("  [OK]")
    except ClientError as e:
        print(f"  [ERROR] R2 upload failed: {e}")


def main() -> None:
    # Discover VinV_1_0 root via existing helper (no hard-coded paths)
    vinv_root = Path(get_vinv_root())
    print(f"[INFO] VinV_1_0 root: {vinv_root}")
    print(f"[INFO] R2 bucket: {R2_BUCKET}")

    for local_path, rel_key in iter_vinv_artifacts(vinv_root):
        upload_to_r2(local_path, rel_key)

    print("[DONE] VinV_1.0 artifacts synced to R2 (where local files existed).")


if __name__ == "__main__":
    main()

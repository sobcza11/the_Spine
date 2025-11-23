"""
Upload the WTI historical inventory parquet to R2.

Local:
    data/us/wti_inv_stor/df_wti_inv_stor_hist.parquet

R2:
    raw/us/wti_inv_stor/df_wti_inv_stor_hist.parquet
"""

from __future__ import annotations
import os
import sys
from pathlib import Path
from io import BytesIO

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from common.r2_client import get_r2_client  # type: ignore


def upload_file(local: Path, key: str):
    bucket = os.getenv("R2_BUCKET")
    if not bucket:
        raise RuntimeError("R2_BUCKET is not set.")

    if not local.exists():
        raise FileNotFoundError(f"Local file not found: {local}")

    client = get_r2_client()

    with local.open("rb") as f:
        client.put_object(Bucket=bucket, Key=key, Body=f)

    print(f"Uploaded {local} → r2://{bucket}/{key}")


def main():
    local = ROOT / "data" / "us" / "wti_inv_stor" / "df_wti_inv_stor_hist.parquet"
    key = "raw/us/wti_inv_stor/df_wti_inv_stor_hist.parquet"
    upload_file(local, key)


if __name__ == "__main__":
    main()


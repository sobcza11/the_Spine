"""
Upload FOMC Minutes canonical parquet to R2.

Run:
python -m spine.jobs.geoscen.upload_fomc_minutes_to_r2
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3

from spine.jobs.geoscen.fomc.fomc_constants import LOCAL_OUTPUT_PATH, R2_KEY


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def main() -> None:
    local_path = Path(LOCAL_OUTPUT_PATH)
    if not local_path.exists():
        raise FileNotFoundError(local_path)

    endpoint = _env("R2_ENDPOINT")
    access_key = _env("R2_ACCESS_KEY_ID")
    secret_key = _env("R2_SECRET_ACCESS_KEY")
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("R2_BUCKET")

    if not bucket:
        raise RuntimeError("Missing R2_BUCKET_NAME or R2_BUCKET")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    client.upload_file(str(local_path), bucket, R2_KEY)
    print(f"[GeoScen:FOMC] uploaded={R2_KEY}")


if __name__ == "__main__":
    main()

    
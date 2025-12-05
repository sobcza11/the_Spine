"""
R2 upload helper for FedSpeak outputs.

CPMAI-friendly design:
- No secrets hardcoded in the repo
- Uses environment variables for credentials
- Clear, minimal interface for uploading a single file
"""

from pathlib import Path
import os

import boto3


def get_r2_client():
    """
    Returns an S3-compatible client configured for Cloudflare R2.

    Requires:
        - R2_ACCESS_KEY_ID
        - R2_SECRET_ACCESS_KEY

    These should be set in your environment (PowerShell, system env, or a .env loader).
    """

    # Your actual Cloudflare account id (from the dash URL)
    account_id = "51f902078bc0e5d7e38896e8a209ccd2"

    # Account-level S3 endpoint (bucket name is passed as Bucket=...)
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "R2_ACCESS_KEY_ID and/or R2_SECRET_ACCESS_KEY are not set. "
            "Set them in your environment before running backfill_r2."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def upload_to_r2(local_path: Path, bucket: str, object_key: str) -> None:
    """
    Upload a local file to R2.

    Args:
        local_path: Path to the local file (parquet/etc.)
        bucket: R2 bucket name (e.g., "thespine-us-hub")
        object_key: Key within the bucket (e.g., "FedSpeak/combined_policy_leaf.parquet")
    """
    client = get_r2_client()
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"Local file does not exist: {local_path}")

    client.upload_file(str(local_path), bucket, object_key)
    print(f"[R2] Uploaded {local_path} -> r2://{bucket}/{object_key}")





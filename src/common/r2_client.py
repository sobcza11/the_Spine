import os
import boto3
from botocore.config import Config


def get_r2_client():
    """
    Create an S3-compatible client for Cloudflare R2.

    Requires the following env vars:
        R2_ACCOUNT_ID
        R2_ACCESS_KEY_ID
        R2_SECRET_ACCESS_KEY
    """
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not account_id:
        raise RuntimeError("R2_ACCOUNT_ID env var is missing or empty – check GitHub Secrets / local env.")
    if not access_key or not secret_key:
        raise RuntimeError("R2_ACCESS_KEY_ID or R2_SECRET_ACCESS_KEY is missing – check GitHub Secrets / local env.")

    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    if endpoint_url == "https://.r2.cloudflarestorage.com":
        # Extra guardrail in case account_id is somehow an empty string
        raise RuntimeError(f"Invalid R2 endpoint_url constructed: {endpoint_url!r}")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )


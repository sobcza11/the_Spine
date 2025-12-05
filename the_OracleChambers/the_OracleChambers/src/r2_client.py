"""
Simple Cloudflare R2 client helpers for the_Spine.

All configuration is taken from environment variables:

    SPINE_R2_ACCOUNT_ID        # Cloudflare account ID (hex)
    SPINE_R2_ACCESS_KEY_ID     # R2 access key ID
    SPINE_R2_SECRET_ACCESS_KEY # R2 secret key
    SPINE_R2_BUCKET            # R2 bucket name (e.g. 'thespine-us-hub')
    SPINE_R2_BASE_PREFIX       # logical root prefix (default: 'the_Spine/')

No local filesystem paths are hard-coded.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.client import Config

R2_ACCOUNT_ID = os.environ.get("SPINE_R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.environ.get("SPINE_R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("SPINE_R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("SPINE_R2_BUCKET", "thespine-us-hub")

# Logical root within the bucket (so structure is configurable outside code)
R2_BASE_PREFIX = os.environ.get("SPINE_R2_BASE_PREFIX", "the_Spine/").lstrip("/")


def _require_config() -> None:
    missing = []
    if not R2_ACCOUNT_ID:
        missing.append("SPINE_R2_ACCOUNT_ID")
    if not R2_ACCESS_KEY_ID:
        missing.append("SPINE_R2_ACCESS_KEY_ID")
    if not R2_SECRET_ACCESS_KEY:
        missing.append("SPINE_R2_SECRET_ACCESS_KEY")
    if missing:
        raise RuntimeError(
            "Missing R2 credentials environment variables: "
            + ", ".join(missing)
        )


def get_r2_client():
    """
    Return a boto3 S3-compatible client pointed at Cloudflare R2.
    """
    _require_config()
    endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )


def make_spine_key(relative: str) -> str:
    """
    Build a full R2 object key under the logical the_Spine root.

    Example:
        relative='vinv/1_0/monthly_panels/vinv_monthly_panel.parquet'
        -> 'the_Spine/vinv/1_0/monthly_panels/vinv_monthly_panel.parquet'
    """
    rel = relative.lstrip("/")
    base = R2_BASE_PREFIX.rstrip("/")
    return f"{base}/{rel}"


def upload_file(
    local_path: Path | str,
    key: str,
    content_type: Optional[str] = None,
) -> None:
    """
    Upload a local file to R2 under the exact object key given.
    """
    client = get_r2_client()
    local_path = Path(local_path)

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    with local_path.open("rb") as f:
        client.put_object(
            Bucket=R2_BUCKET,
            Key=key,
            Body=f,
            **extra_args,
        )


def upload_spine_artifact(
    local_path: Path | str,
    relative_key: str,
    content_type: Optional[str] = None,
) -> None:
    """
    Upload a file to the_Spine logical namespace in R2.

    relative_key is under the logical the_Spine root, e.g.:

        'vinv/1_0/monthly_panels/vinv_monthly_panel.parquet'
    """
    full_key = make_spine_key(relative_key)
    upload_file(local_path, full_key, content_type=content_type)


def touch_placeholder(relative_prefix: str, name: str = "_placeholder") -> None:
    """
    Create a zero-byte placeholder object at the given prefix. Useful to
    materialize directory-like structures.

        relative_prefix = 'vinv/1_0/monthly_panels/'
    """
    client = get_r2_client()
    key = make_spine_key(relative_prefix.rstrip("/") + "/" + name)
    client.put_object(
        Bucket=R2_BUCKET,
        Key=key,
        Body=b"",
        ContentType="text/plain",
    )

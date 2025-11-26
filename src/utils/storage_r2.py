import os
import logging

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import S3UploadFailedError

log = logging.getLogger("R2")


def r2_client():
    """
    Build an S3-compatible client for Cloudflare R2.

    Expects env vars:
        R2_ENDPOINT
        R2_ACCESS_KEY
        R2_SECRET_KEY
    """
    endpoint = os.getenv("R2_ENDPOINT")
    access_key = os.getenv("R2_ACCESS_KEY")
    secret_key = os.getenv("R2_SECRET_KEY")

    missing = [
        name
        for name, val in [
            ("R2_ENDPOINT", endpoint),
            ("R2_ACCESS_KEY", access_key),
            ("R2_SECRET_KEY", secret_key),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(
            f"Missing R2 configuration in environment: {', '.join(missing)}"
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def upload_file_to_r2(local_path: str, r2_key: str) -> None:
    """
    Upload a file to the default US bucket in R2.

    If env vars are missing OR upload fails (SignatureDoesNotMatch, etc.),
    log a warning and continue without raising so local builds never break.
    """
    bucket = os.getenv("R2_BUCKET_US")
    if not bucket:
        log.warning(
            "[R2] R2_BUCKET_US not set; skipping upload of %s to key=%s",
            local_path,
            r2_key,
        )
        return

    try:
        client = r2_client()
    except RuntimeError as exc:
        log.warning("[R2] %s; skipping upload of %s", exc, local_path)
        return

    try:
        log.info(
            "[R2] Uploading %s to bucket=%s, key=%s",
            local_path,
            bucket,
            r2_key,
        )
        client.upload_file(local_path, bucket, r2_key)
        log.info("[R2] Upload complete.")
    except (ClientError, S3UploadFailedError, Exception) as exc:
        log.warning(
            "[R2] Upload failed for %s → %s/%s (%s). Keeping local file only.",
            local_path,
            bucket,
            r2_key,
            exc,
        )

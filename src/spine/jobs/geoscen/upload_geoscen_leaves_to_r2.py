"""
Upload GeoScen leaf outputs to Cloudflare R2 with hash/no-op logic.

Run:
python -m spine.jobs.geoscen.upload_geoscen_leaves_to_r2
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


LEAVES = [
    {
        "local_path": "data/geoscen/signals/geoscen_beige_book_signals_v1.parquet",
        "r2_key": "spine_us/leaves/geoscen/geoscen_beige_book_signals_v1.parquet",
    },
    {
        "local_path": "data/geoscen/signals/geoscen_beige_book_monthly_aggregates_v1.parquet",
        "r2_key": "spine_us/leaves/geoscen/geoscen_beige_book_monthly_aggregates_v1.parquet",
    },
]


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _client():
    return boto3.client(
        "s3",
        endpoint_url=_env("R2_ENDPOINT"),
        aws_access_key_id=_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=_env("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def _bucket() -> str:
    return os.getenv("R2_BUCKET_NAME") or _env("R2_BUCKET")


def _remote_hash(client, bucket: str, key: str) -> str | None:
    try:
        obj = client.head_object(Bucket=bucket, Key=key)
        return obj.get("Metadata", {}).get("sha256")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def main() -> None:
    client = _client()
    bucket = _bucket()

    uploaded = 0
    unchanged = 0

    for leaf in LEAVES:
        local_path = Path(leaf["local_path"])
        r2_key = leaf["r2_key"]

        if not local_path.exists():
            raise FileNotFoundError(local_path)

        local_hash = _sha256_file(local_path)
        remote_hash = _remote_hash(client, bucket, r2_key)

        if remote_hash == local_hash:
            print(f"[GeoScen:R2] unchanged={r2_key}")
            unchanged += 1
            continue

        client.upload_file(
            str(local_path),
            bucket,
            r2_key,
            ExtraArgs={
                "Metadata": {
                    "sha256": local_hash,
                    "source": "the_spine_geoscen",
                    "leaf": Path(r2_key).name,
                }
            },
        )

        print(f"[GeoScen:R2] uploaded={r2_key}")
        print(f"[GeoScen:R2] sha256={local_hash}")
        uploaded += 1

    print(f"[GeoScen:R2] complete uploaded={uploaded} unchanged={unchanged}")


if __name__ == "__main__":
    main()


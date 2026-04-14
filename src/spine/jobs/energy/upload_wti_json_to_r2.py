from __future__ import annotations

import mimetypes
import os
from pathlib import Path

import boto3


BUCKET = "thespine-us-hub"

LOCAL_FILES = {
    "artifacts/json/wti_price_data.json": "spine_us/serving/wti/wti_price_data.json",
    "artifacts/json/wti_inventory_data.json": "spine_us/serving/wti/wti_inventory_data.json",
}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def upload_file(s3_client, local_path: str, bucket_key: str) -> None:
    path = Path(local_path)
    if not path.exists():
        raise FileNotFoundError(f"Local file not found: {path}")

    content_type, _ = mimetypes.guess_type(path.name)
    if not content_type:
        content_type = "application/json"

    with path.open("rb") as f:
        s3_client.put_object(
            Bucket=BUCKET,
            Key=bucket_key,
            Body=f.read(),
            ContentType=content_type,
        )

    print(f"uploaded: {path} -> s3://{BUCKET}/{bucket_key}")


def main() -> None:
    s3 = get_s3_client()

    for local_path, bucket_key in LOCAL_FILES.items():
        upload_file(s3, local_path, bucket_key)

    print("WTI JSON upload complete.")


if __name__ == "__main__":
    main()

    
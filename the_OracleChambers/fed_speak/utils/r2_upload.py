from pathlib import Path
import os
import boto3

def get_r2_client():
    account_id = "51f902078bc0e5d7e38896e8a209ccd2"
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "Missing R2_ACCESS_KEY_ID or R2_SECRET_ACCESS_KEY environment variables."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

def upload_to_r2(local_path: Path, bucket: str, object_key: str) -> None:
    client = get_r2_client()
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"Local file does not exist: {local_path}")

    client.upload_file(str(local_path), bucket, object_key)
    print(f"[R2] Uploaded {local_path} -> r2://{bucket}/{object_key}")

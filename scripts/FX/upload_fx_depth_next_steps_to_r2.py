import os
import boto3
from pathlib import Path

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET", "spine_us")

LOCAL_DIR = Path("dist/fx_depth")
R2_PREFIX = "fx_depth"

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
)

for file in LOCAL_DIR.glob("*.json"):
    key = f"{R2_PREFIX}/{file.name}"

    s3.upload_file(
        str(file),
        R2_BUCKET,
        key,
        ExtraArgs={
            "ContentType": "application/json",
            "CacheControl": "public, max-age=300",
        },
    )

    print(f"uploaded r2://{R2_BUCKET}/{key}")
    
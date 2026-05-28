import os
from pathlib import Path
import boto3

local_path = Path("data/serving/rates/rates_panel_v1.html")
object_key = "spine_us/serving/rates/rates_panel_v1.html"

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["R2_ENDPOINT"],
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    region_name=os.getenv("R2_REGION", "auto"),
)

s3.upload_file(
    str(local_path),
    os.environ["R2_BUCKET_NAME"],
    object_key,
    ExtraArgs={
        "ContentType": "text/html",
        "CacheControl": "no-store",
    },
)

print(f"PASS | Uploaded {local_path} -> {object_key}")


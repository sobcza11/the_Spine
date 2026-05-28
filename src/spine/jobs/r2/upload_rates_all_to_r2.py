import os
from pathlib import Path
import boto3

FILES = [
    ("data/serving/rates/rates_panel_v1.html", "spine_us/serving/rates/rates_panel_v1.html", "text/html"),
    ("data/serving/rates/rates_us_yield_family.json", "spine_us/serving/rates/rates_us_yield_family.json", "application/json"),
    ("data/serving/rates/rates_us_yield_family_latest.json", "spine_us/serving/rates/rates_us_yield_family_latest.json", "application/json"),
    ("data/serving/rates/rates_zt_latest.json", "spine_us/serving/rates/rates_zt_latest.json", "application/json"),
    ("data/serving/rates/rates_zt_panel.json", "spine_us/serving/rates/rates_zt_panel.json", "application/json"),
]

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["R2_ENDPOINT"],
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    region_name=os.getenv("R2_REGION", "auto"),
)

for local, key, content_type in FILES:
    s3.upload_file(
        str(Path(local)),
        os.environ["R2_BUCKET_NAME"],
        key,
        ExtraArgs={"ContentType": content_type, "CacheControl": "no-store"},
    )
    print(f"UPLOADED | {local} -> {key}")

print("PASS | Uploaded full RATES website bundle")


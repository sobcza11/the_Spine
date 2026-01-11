import os
import boto3
from botocore.config import Config

endpoint = os.getenv("R2_ENDPOINT_URL")
bucket   = os.getenv("R2_BUCKET", "thespine-us-hub")
region   = os.getenv("R2_REGION", "auto")
key      = os.getenv("R2_ACCESS_KEY_ID")
secret   = os.getenv("R2_SECRET_ACCESS_KEY")

missing = [k for k,v in {
    "R2_ENDPOINT_URL": endpoint,
    "R2_ACCESS_KEY_ID": key,
    "R2_SECRET_ACCESS_KEY": secret,
}.items() if not v]
if missing:
    raise SystemExit(f"Missing env vars: {missing}")

cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=key,
    aws_secret_access_key=secret,
    region_name=region,
    config=cfg,
)

prefix = "the_Spine/vinv/1_0/"
resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=200)

print("Bucket:", bucket)
print("Prefix:", prefix)
print("Keys:")
for obj in resp.get("Contents", []):
    print(" -", obj["Key"])

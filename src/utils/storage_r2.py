import os
from dotenv import load_dotenv
import boto3

load_dotenv()  # loads variables from .env


def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY"],
        aws_secret_access_key=os.environ["R2_SECRET_KEY"],
    )


def upload_us(local_path: str, remote_key: str):
    client = r2_client()
    client.upload_file(local_path, os.environ["R2_BUCKET_US"], remote_key)


def list_us(prefix: str = ""):
    client = r2_client()
    resp = client.list_objects_v2(
        Bucket=os.environ["R2_BUCKET_US"],
        Prefix=prefix
    )
    return resp.get("Contents", [])

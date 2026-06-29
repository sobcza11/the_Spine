from pathlib import Path
import os

import boto3


LOCAL_PATH = Path("data/geoscen/cb/macro_cb_canonical_v1.parquet")
R2_KEY = "spine_us/geoscen/cb/macro_cb_canonical_v1.parquet"


def run():
    endpoint = os.getenv("R2_ENDPOINT") or os.getenv("R2_ENDPOINT_URL")
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("R2_BUCKET")

    if not endpoint:
        raise RuntimeError("Missing R2_ENDPOINT or R2_ENDPOINT_URL")

    if not bucket:
        raise RuntimeError("Missing R2_BUCKET_NAME or R2_BUCKET")

    if not LOCAL_PATH.exists():
        raise FileNotFoundError(LOCAL_PATH)

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )

    client.upload_file(str(LOCAL_PATH), bucket, R2_KEY)

    print(f"Uploaded: {R2_KEY}")


if __name__ == "__main__":
    run()


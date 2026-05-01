import os
import boto3

from spine.jobs.geoscen.cb.boj.boj_constants import (
    COMBINED_CANONICAL_OUTPUT,
    R2_PREFIX,
)

R2_KEY = f"{R2_PREFIX}/boj_combined_canonical_v1.parquet"


def upload_boj_combined_canonical_to_r2():
    required_env = [
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
        "R2_ENDPOINT",
        "R2_REGION",
    ]

    missing = [x for x in required_env if not os.getenv(x)]
    if missing:
        raise ValueError(f"[FAIL] Missing R2 env vars: {missing}")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION"),
    )

    s3.upload_file(
        Filename=str(COMBINED_CANONICAL_OUTPUT),
        Bucket=os.getenv("R2_BUCKET_NAME"),
        Key=R2_KEY,
    )

    print("[OK] Uploaded BoJ combined canonical to R2")
    print(f"[R2_KEY] {R2_KEY}")


if __name__ == "__main__":
    upload_boj_combined_canonical_to_r2()


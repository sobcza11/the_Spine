import boto3
import os

FILES = [
    (
        "data/geoscen/signals/macro_cb_terms_v1.parquet",
        "spine_global/leaves/geoscen/macro_cb_terms_v1.parquet",
    ),
    (
        "data/geoscen/signals/macro_cb_monthly_aggregates_v1.parquet",
        "spine_global/leaves/geoscen/macro_cb_monthly_aggregates_v1.parquet",
    ),
]


def run():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ["R2_REGION"],
    )

    bucket = os.environ["R2_BUCKET_NAME"]

    for local_path, key in FILES:
        s3.upload_file(local_path, bucket, key)
        print(f"Uploaded: {key}")


if __name__ == "__main__":
    run()


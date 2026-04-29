import boto3
import os

OUTPUT_PATH = "data/geoscen/cb/macro_cb_canonical_v1.parquet"


def run():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ["R2_REGION"]
    )

    bucket = os.environ["R2_BUCKET_NAME"]

    s3.upload_file(
        OUTPUT_PATH,
        bucket,
        "spine_global/leaves/geoscen/macro_cb_canonical_v1.parquet"
    )

    print("Uploaded macro_cb canonical")


if __name__ == "__main__":
    run()


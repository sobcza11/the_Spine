import boto3
import os

LOCAL_PATH = "data/geoscen/signals/isovector_macro_cb_view_v1.parquet"
R2_KEY = "spine_global/isovector/geoscen/isovector_macro_cb_view_v1.parquet"


def run():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ["R2_REGION"],
    )

    bucket = os.environ["R2_BUCKET_NAME"]

    s3.upload_file(LOCAL_PATH, bucket, R2_KEY)

    print(f"Uploaded: {R2_KEY}")


if __name__ == "__main__":
    run()


import boto3
import os

from .ecb_constants import ECB_OUTPUT_ACCOUNTS_PATH

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
        ECB_OUTPUT_ACCOUNTS_PATH,
        bucket,
        "spine_global/leaves/geoscen/ecb_accounts_t1.parquet"
    )

    print("Uploaded ECB Accounts")


if __name__ == "__main__":
    run()


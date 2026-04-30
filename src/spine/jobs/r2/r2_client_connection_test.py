import os

import boto3


def main():
    required = [
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
        "R2_ENDPOINT",
        "R2_REGION",
    ]

    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise EnvironmentError(f"Missing required R2 env vars: {missing}")

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION"),
    )

    bucket = os.getenv("R2_BUCKET_NAME")

    client.head_bucket(Bucket=bucket)

    print("R2_CLIENT_CONNECTION_OK")
    print(f"bucket={bucket}")
    print(f"endpoint={os.getenv('R2_ENDPOINT')}")


if __name__ == "__main__":
    main()

    
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

    bucket = os.getenv("R2_BUCKET_NAME")
    key = "spine_control/smoke_tests/r2_upload_smoke_test.txt"
    body = b"r2 upload smoke test ok"

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION"),
    )

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/plain",
    )

    response = client.get_object(Bucket=bucket, Key=key)
    read_back = response["Body"].read()

    if read_back != body:
        raise ValueError("R2 smoke test failed: uploaded body != downloaded body")

    print("R2_UPLOAD_SMOKE_TEST_OK")
    print(f"bucket={bucket}")
    print(f"key={key}")


if __name__ == "__main__":
    main()

    
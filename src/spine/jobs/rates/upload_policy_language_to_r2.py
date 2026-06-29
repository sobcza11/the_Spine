from pathlib import Path
import os

import boto3


ROOT = Path.cwd()
SERVING = ROOT / "data" / "serving" / "rates"


def main():
    endpoint = os.getenv("R2_ENDPOINT") or os.getenv("R2_ENDPOINT_URL")
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("R2_BUCKET")

    if not endpoint:
        raise RuntimeError("Missing R2_ENDPOINT or R2_ENDPOINT_URL")

    if not bucket:
        raise RuntimeError("Missing R2_BUCKET_NAME or R2_BUCKET")

    files = sorted(SERVING.glob("*_policy_language_latest.json"))

    if not files:
        print("No policy-language JSON files found. Skipping upload.")
        return

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )

    for file in files:
        key = f"spine_us/serving/rates/{file.name}"
        client.upload_file(str(file), bucket, key)
        print(f"Uploaded: {key}")


if __name__ == "__main__":
    main()

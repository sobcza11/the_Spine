from pathlib import Path
import os
import boto3

REPO_ROOT = Path.cwd()

LOCAL_PATH = REPO_ROOT / "data" / "serving" / "fx" / "fx_depth_serving_v1.json"
R2_KEY = "spine_us/serving/fx/fx_depth_serving_v1.json"

def main():
    if not LOCAL_PATH.exists():
        raise FileNotFoundError(f"Missing FX DEPTH serving file: {LOCAL_PATH}")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("R2_REGION", "auto"),
    )

    s3.upload_file(
        str(LOCAL_PATH),
        os.environ["R2_BUCKET_NAME"],
        R2_KEY
    )

    print(f"UPLOADED FX DEPTH: {LOCAL_PATH} -> {R2_KEY}")

if __name__ == "__main__":
    main()

    
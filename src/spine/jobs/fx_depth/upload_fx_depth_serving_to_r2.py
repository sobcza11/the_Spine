from pathlib import Path
import os
import boto3

REPO_ROOT = Path.cwd()

FILES = [
    (
        REPO_ROOT / "data" / "serving" / "fx" / "fx_depth_serving_v1.json",
        "spine_us/serving/fx/fx_depth_serving_v1.json",
    ),
    (
        REPO_ROOT / "data" / "serving" / "fx" / "fx_spreads_data.json",
        "spine_us/serving/fx/fx_spreads_data.json",
    ),
]


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("R2_REGION", "auto"),
    )

    for local_path, r2_key in FILES:
        if not local_path.exists():
            raise FileNotFoundError(f"Missing file: {local_path}")

        s3.upload_file(
            str(local_path),
            os.environ["R2_BUCKET_NAME"],
            r2_key,
        )

        print(f"UPLOADED: {local_path} -> {r2_key}")


if __name__ == "__main__":
    main()

    
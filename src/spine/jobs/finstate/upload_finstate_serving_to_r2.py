from pathlib import Path
import os
import boto3

ROOT = Path.cwd()

FILES = [
    "finstate_sigma_rank.json",
    "finstate_universe_metrics_v1.json",
    "finstate_global_lite_metrics_v1.json",
]

def main():
    bucket = os.environ["R2_BUCKET_NAME"]
    endpoint = os.environ["R2_ENDPOINT"]
    access_key = os.environ["R2_ACCESS_KEY_ID"]
    secret_key = os.environ["R2_SECRET_ACCESS_KEY"]
    region = os.environ.get("R2_REGION", "auto")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    uploaded = 0

    for name in FILES:
        local = ROOT / "data" / "serving" / "finstate" / name

        if not local.exists():
            print(f"SKIP MISSING: {local}")
            continue

        key = f"spine_us/serving/finstate/{name}"

        s3.upload_file(
            str(local),
            bucket,
            key,
            ExtraArgs={"ContentType": "application/json"},
        )

        uploaded += 1
        print(f"UPLOADED: {local} -> {key}")

    if uploaded == 0:
        raise FileNotFoundError("No FINSTATE serving files found to upload.")

if __name__ == "__main__":
    main()

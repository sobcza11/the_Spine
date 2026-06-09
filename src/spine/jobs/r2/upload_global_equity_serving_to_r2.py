import os
from pathlib import Path

import boto3

ROOT = Path.cwd()

UPLOADS = {
    "data/serving/equities/global_equity_region_panel.json":
        "spine_us/serving/equities/global_equity_region_panel.json",
    "data/serving/equities/global_equity_region_latest.json":
        "spine_us/serving/equities/global_equity_region_latest.json",
}


def main():
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("R2_REGION", "auto"),
    )

    bucket = os.environ["R2_BUCKET_NAME"]

    for local_rel, key in UPLOADS.items():
        local = ROOT / local_rel

        if not local.exists():
            raise FileNotFoundError(f"Missing file: {local}")

        client.upload_file(
            str(local),
            bucket,
            key,
            ExtraArgs={"ContentType": "application/json"},
        )

        print(f"UPLOADED: {local} -> {key}")


if __name__ == "__main__":
    main()

    
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


REPO_ROOT = Path.cwd()

UPLOADS = {
    # RATES
    "data/rates/zt/rates_core_zt.parquet": "spine_us/serving/rates/rates_core_zt.parquet",
    "data/macro/serving/rates/rates_serving_panel.parquet": "spine_us/serving/rates/rates_serving_panel.parquet",
    "data/macro/serving/rates/rates_latest.json": "spine_us/serving/rates/rates_latest.json",

    # FX
    "data/serving/fx/fx_zt_v1.parquet": "spine_us/serving/fx/fx_zt_v1.parquet",
    "data/serving/fx/fx_latest.json": "spine_us/serving/fx/fx_latest.json",

    # C-FL
    "data/processed/cflow/cfl_cot_panel_clean.parquet": "spine_us/serving/cflow/cfl_cot_panel_clean.parquet",
    "data/processed/cflow/cfl_zt_panel.parquet": "spine_us/serving/cflow/cfl_zt_panel.parquet",
    "data/processed/cflow/cfl_factor_panel.parquet": "spine_us/serving/cflow/cfl_factor_panel.parquet",
    "data/serving/cflow/commflow_panel.json": "spine_us/serving/cflow/cflow_panel.json",
    "data/serving/cflow/commflow_latest.json": "spine_us/serving/cflow/cflow_latest.json",

    # EQUITIES
    "data/serving/equities/equities_zt.parquet": "spine_us/serving/equities/equities_zt.parquet",
    "data/serving/equities/equities_risk_expression_panel.parquet": "spine_us/serving/equities/equities_risk_expression_panel.parquet",

    #WTI-Inv.
    "data/serving/fx/fx_depth_serving_v1.json": "spine_us/serving/fx/fx_depth_serving_v1.json",
    "data/serving/fx/fx_zt_v1.parquet": "spine_us/serving/fx/fx_zt_v1.parquet",
    "data/serving/fx/fx_latest.json": "spine_us/serving/fx/fx_latest.json",

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

    missing = []

    for local_rel, r2_key in UPLOADS.items():
        local_path = REPO_ROOT / local_rel

        if not local_path.exists():
            missing.append(str(local_path))
            continue

        try:
            client.upload_file(str(local_path), bucket, r2_key)
            print(f"OK | uploaded {local_rel} -> {r2_key}")
        except ClientError as e:
            raise RuntimeError(f"Upload failed for {local_rel}: {e}") from e

    if missing:
        print("\nMISSING LOCAL FILES:")
        for m in missing:
            print(m)
        raise FileNotFoundError("Some expected upload files are missing.")

    print("\nOK | Core module outputs uploaded to R2")


if __name__ == "__main__":
    main()
    
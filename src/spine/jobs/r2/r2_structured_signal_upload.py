import os
from pathlib import Path

import boto3


ROOT = Path.cwd()

UPLOAD_MAP = {
    "data/geoscen/signals/macro_cb_signals_v1.parquet": "geoscen/signals/macro_cb_signals_v1.parquet",
    "data/geoscen/signals/macro_cb_oc_signals_v1.parquet": "geoscen/signals/macro_cb_oc_signals_v1.parquet",
    "data/geoscen/signals/isovector_macro_cb_view_v1.parquet": "isovector/macro_cb_view/isovector_macro_cb_view_v1.parquet",
    "data/geoscen/signals/isovector_macro_cb_rates_join_v1.parquet": "isovector/macro_cb_rates_join/isovector_macro_cb_rates_join_v1.parquet",
    "data/geoscen/pmi/signals/pmi_geoscen_zt_input_v1.parquet": "geoscen/pmi/signals/pmi_geoscen_zt_input_v1.parquet",
    "data/geoscen/pmi/signals/pmi_commentary_tags_v1.parquet": "geoscen/pmi/signals/pmi_commentary_tags_v1.parquet",
    "data/geoscen/pmi/signals/pmi_commentary_numeric_overlay_v1.parquet": "geoscen/pmi/signals/pmi_commentary_numeric_overlay_v1.parquet",
}


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
        raise EnvironmentError(f"Missing R2 env vars: {missing}")

    bucket = os.getenv("R2_BUCKET_NAME")

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION"),
    )

    uploaded = 0
    missing_files = []

    for local_rel, r2_key in UPLOAD_MAP.items():
        local_path = ROOT / local_rel

        if not local_path.exists():
            missing_files.append(local_rel)
            print(f"MISSING {local_rel}")
            continue

        client.upload_file(
            Filename=str(local_path),
            Bucket=bucket,
            Key=r2_key,
        )

        uploaded += 1
        print(f"UPLOADED s3://{bucket}/{r2_key}")

    print("R2_STRUCTURED_SIGNAL_UPLOAD_COMPLETE")
    print(f"bucket={bucket}")
    print(f"uploaded={uploaded}")
    print(f"missing={len(missing_files)}")

    if missing_files:
        raise SystemExit(f"Missing required structured files: {missing_files}")


if __name__ == "__main__":
    main()
    
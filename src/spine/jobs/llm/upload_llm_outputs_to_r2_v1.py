import os
from pathlib import Path

import boto3


ROOT = Path.cwd()

UPLOAD_MAP = {
    "data/llm/oraclechambers/oraclechambers_brief_v1.md": "oraclechambers/briefs/v1/oraclechambers_brief_v1.md",
    "data/llm/qualayer/qualayer_packet_v1.json": "oraclechambers/qualayer/v1/qualayer_packet_v1.json",
    "data/llm/logs/llm_run_log_v1.jsonl": "oraclechambers/logs/v1/llm_run_log_v1.jsonl",
    "data/llm/event_monitor/llm_event_monitor_v1.jsonl": "oraclechambers/event_monitor/v1/llm_event_monitor_v1.jsonl",
}


def env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=env("R2_ENDPOINT"),
        aws_access_key_id=env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
        region_name=os.getenv("R2_REGION", "auto"),
    )


def main():
    bucket = env("R2_BUCKET_NAME")
    client = s3_client()

    uploaded = 0
    missing = []

    for local_rel, r2_key in UPLOAD_MAP.items():
        local_path = ROOT / local_rel

        if not local_path.exists():
            missing.append(local_rel)
            print(f"MISSING {local_rel}")
            continue

        client.upload_file(
            Filename=str(local_path),
            Bucket=bucket,
            Key=r2_key,
        )

        uploaded += 1
        print(f"UPLOADED s3://{bucket}/{r2_key}")

    print("LLM_OUTPUTS_R2_UPLOAD_COMPLETE")
    print(f"bucket={bucket}")
    print(f"uploaded={uploaded}")
    print(f"missing={len(missing)}")

    if missing:
        raise SystemExit(f"Missing LLM output files: {missing}")


if __name__ == "__main__":
    main()

    
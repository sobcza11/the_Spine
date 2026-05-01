import hashlib
import json
import os
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd


ROOT = Path.cwd()
MANIFEST_PATH = ROOT / "config" / "llm" / "approved_r2_inputs_v1.json"

OUT_DIR = ROOT / "data" / "llm"
CACHE_DIR = OUT_DIR / "approved_inputs"
EVENT_DIR = OUT_DIR / "event_monitor"
LOG_DIR = OUT_DIR / "logs"

EVENT_PATH = EVENT_DIR / "llm_event_monitor_v1.jsonl"
LOG_PATH = LOG_DIR / "llm_run_log_v1.jsonl"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


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


def write_jsonl(path: Path, record: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    EVENT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    bucket = env("R2_BUCKET_NAME")
    client = s3_client()

    run_id = f"llm_loader_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    loaded = {}

    for name, key in manifest["approved_inputs"].items():
        obj = client.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()

        df = pd.read_parquet(BytesIO(raw))
        out_path = CACHE_DIR / f"{name}.parquet"
        df.to_parquet(out_path, index=False)

        loaded[name] = {
            "r2_key": key,
            "local_path": str(out_path),
            "rows": int(len(df)),
            "columns": list(df.columns),
            "sha256": sha256_bytes(raw),
        }

    packet = {
        "run_id": run_id,
        "created_at": utc_now(),
        "manifest_version": manifest["version"],
        "layer": manifest["layer"],
        "approved_inputs_loaded": loaded,
    }

    packet_path = OUT_DIR / "approved_inputs_packet_v1.json"
    packet_path.write_text(json.dumps(packet, indent=2, default=str), encoding="utf-8")

    write_jsonl(EVENT_PATH, {
        "event_type": "llm_approved_inputs_loaded",
        "run_id": run_id,
        "created_at": utc_now(),
        "manifest_version": manifest["version"],
        "input_count": len(loaded),
    })

    write_jsonl(LOG_PATH, packet)

    print("LLM_APPROVED_R2_INPUTS_LOADED")
    print(f"run_id={run_id}")
    print(f"inputs={len(loaded)}")
    print(f"packet={packet_path}")


if __name__ == "__main__":
    main()


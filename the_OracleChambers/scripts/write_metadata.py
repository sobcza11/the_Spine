import json
from datetime import datetime
from pathlib import Path
import hashlib
from typing import Iterable


def compute_schema_hash(columns: Iterable[str]) -> str:
    """
    Deterministic schema hash based on column names.
    Ensures schema consistency across pipeline runs.
    """
    cols_sorted = tuple(sorted(columns))
    return hashlib.md5(str(cols_sorted).encode("utf-8")).hexdigest()[:12]


def write_metadata(
    artifact_path: Path,
    artifact_name: str,
    artifact_type: str,
    channel: str,
    data_date: str,
    df,
    pipeline_config_version: str,
    source_reference: str = "",
    notes: str = "",
):
    """
    Writes metadata JSON for any artifact according to CPMAI rules.
    """
    meta = {
        "artifact_name": artifact_name,
        "artifact_type": artifact_type,
        "channel": channel,
        "version": "v1.0.0",
        "data_date": data_date,
        "_ingest_timestamp_utc": datetime.utcnow().isoformat(),
        "process_timestamp_utc": datetime.utcnow().isoformat(),
        "source_reference": source_reference,
        "record_count": len(df),
        "schema_columns": list(df.columns),
        "schema_hash": compute_schema_hash(df.columns),
        "pipeline_config_version": pipeline_config_version,
        "notes": notes,
    }

    meta_path = artifact_path.with_suffix(".meta.json")

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return meta_path

from __future__ import annotations

import hashlib
from typing import Any, Mapping, Sequence

from spine.jobs.geoscen.eis.contracts import LineageRecord, canonical_json, utc_now_iso
from spine.jobs.geoscen.eis.credentials import redact_mapping

DEFAULT_TRANSFORMATION_VERSION = "geoscen-eis-stage1"


def sha256_bytes(payload: bytes | None) -> str | None:
    if payload is None:
        return None
    return hashlib.sha256(payload).hexdigest()


def sha256_canonical(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def request_metadata_hash(request_metadata: Mapping[str, Any]) -> str:
    return sha256_canonical(redact_mapping(request_metadata))


def normalized_payload_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    return sha256_canonical(list(rows))


def build_lineage_record(
    *,
    provider: str,
    operation: str,
    request_metadata: Mapping[str, Any],
    normalized_rows: Sequence[Mapping[str, Any]],
    raw_sha256: str | None = None,
    source_payload: str,
    source_artifact: str,
    source_run_ts: str | None = None,
    transformation_version: str = DEFAULT_TRANSFORMATION_VERSION,
) -> LineageRecord:
    return LineageRecord(
        source_payload=source_payload,
        source_artifact=source_artifact,
        source_run_ts=source_run_ts or utc_now_iso(),
        provider=provider,
        operation=operation,
        request_hash=request_metadata_hash(request_metadata),
        raw_sha256=raw_sha256,
        normalized_sha256=normalized_payload_hash(normalized_rows),
        transformation_version=transformation_version,
    )

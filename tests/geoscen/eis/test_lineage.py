from __future__ import annotations

from spine.jobs.geoscen.eis.lineage import (
    build_lineage_record,
    normalized_payload_hash,
    request_metadata_hash,
    sha256_bytes,
)


def test_lineage_hashes_are_deterministic_and_secret_safe() -> None:
    metadata = {"b": 2, "api_key": "secret", "a": 1}
    assert request_metadata_hash(metadata) == request_metadata_hash({"a": 1, "b": 2, "api_key": "other"})
    rows = [{"date": "2026-01-01", "value": 1}]
    assert normalized_payload_hash(rows) == normalized_payload_hash(rows)
    raw_hash = sha256_bytes(b"raw")
    lineage = build_lineage_record(
        provider="fred",
        operation="latest",
        request_metadata=metadata,
        normalized_rows=rows,
        raw_sha256=raw_hash,
        source_payload="fred",
        source_artifact="artifact",
        source_run_ts="2026-01-01T00:00:00Z",
        transformation_version="test-v1",
    )
    payload = lineage.to_dict()
    assert payload["raw_sha256"] == raw_hash
    assert payload["transformation_version"] == "test-v1"

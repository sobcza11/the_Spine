from __future__ import annotations

import json

import pytest

from spine.jobs.geoscen.eis.contracts import (
    ConnectorRequest,
    ConnectorResponse,
    ConnectorStatus,
    LineageRecord,
    SourceMetadata,
    ValidationResult,
    canonical_json,
    utc_now_iso,
)
from spine.jobs.geoscen.eis.exceptions import RequestValidationError


def test_contract_serialization_is_deterministic_and_secret_safe() -> None:
    request = ConnectorRequest(
        provider="BLS",
        operation="Fetch",
        parameters={"series": "X", "api_key": "secret"},
        metadata={"authorization": "Bearer secret"},
        correlation_id="fixed",
        requested_at="2026-01-01T00:00:00Z",
    )
    first = canonical_json(request)
    second = canonical_json(request)
    assert first == second
    assert "secret" not in first
    assert "BLS" not in first
    assert json.loads(first)["provider"] == "bls"


def test_utc_timestamp_format() -> None:
    assert utc_now_iso().endswith("Z")


def test_mutable_defaults_are_not_shared() -> None:
    a = ConnectorRequest(provider="fred", operation="latest")
    b = ConnectorRequest(provider="fred", operation="latest")
    assert a.parameters is not b.parameters
    assert a.metadata is not b.metadata


def test_executable_objects_rejected() -> None:
    with pytest.raises(RequestValidationError):
        ConnectorRequest(provider="fred", operation="latest", parameters={"bad": lambda: None})


def test_response_excludes_raw_body() -> None:
    source = SourceMetadata(provider="fred", endpoint="https://example.com")
    lineage = LineageRecord(
        source_payload="payload",
        source_artifact="artifact",
        source_run_ts="2026-01-01T00:00:00Z",
        provider="fred",
        operation="latest",
        request_hash="a",
        raw_sha256="b",
        normalized_sha256="c",
        transformation_version="v1",
    )
    response = ConnectorResponse(
        provider="fred",
        operation="latest",
        status=ConnectorStatus.SUCCESS,
        retrieved_at="2026-01-01T00:00:00Z",
        request_metadata={"api_key": "secret"},
        source_metadata=source,
        lineage=lineage,
        normalized_rows=[{"date": "2026-01-01", "value": 1}],
        validation=ValidationResult(valid=True, row_count=1),
    )
    payload = canonical_json(response)
    assert "raw_body" not in payload
    assert "secret" not in payload
    assert json.loads(payload)["normalized_rows"][0]["value"] == 1

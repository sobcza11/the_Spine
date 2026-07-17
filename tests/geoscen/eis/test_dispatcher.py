from __future__ import annotations

import pytest

from spine.jobs.geoscen.eis.contracts import (
    ConnectorRequest,
    SourceMetadata,
    UpstreamResponse,
    ValidationResult,
    utc_now_iso,
)
from spine.jobs.geoscen.eis.dispatcher import dispatch
from spine.jobs.geoscen.eis.exceptions import (
    CredentialError,
    DispatchError,
    ProviderNotFoundError,
    ResponseValidationError,
    UnsupportedOperationError,
)
from spine.jobs.geoscen.eis.registry import ConnectorRegistry


class MockConnector:
    provider = "mock"
    supported_operations = ("latest",)
    credential_specification = {}

    def validate_request(self, request):
        return ValidationResult(True)

    def fetch(self, request, http_client, credentials):
        return UpstreamResponse(
            url="https://api.example.com/data",
            method="GET",
            status_code=200,
            headers={"content-type": "application/json"},
            content=b'{"rows":[{"date":"2026-01-01","value":1}]}',
            retrieved_at=utc_now_iso(),
        )

    def parse_response(self, response, request):
        return {
            "source_metadata": SourceMetadata(
                provider=request.provider,
                endpoint=response.url,
                dataset="mock",
                retrieval_timestamp=response.retrieved_at,
                content_type=response.content_type,
                upstream_status=response.status_code,
            ),
            "normalized_rows": [{"date": "2026-01-01", "value": 1}],
            "source_payload": "mock-payload",
            "source_artifact": "mock-artifact",
            "validation": ValidationResult(True, row_count=1),
        }


class InvalidRequestConnector(MockConnector):
    def validate_request(self, request):
        return ValidationResult(False, errors=("bad",))


class InvalidResponseConnector(MockConnector):
    def parse_response(self, response, request):
        return {"normalized_rows": ["bad"], "validation": ValidationResult(False, errors=("bad",))}


class BadFetchConnector(MockConnector):
    def fetch(self, request, http_client, credentials):
        return object()


class CredentialConnector(MockConnector):
    credential_specification = {"MOCK_API_KEY": True}


def registry_with(connector) -> ConnectorRegistry:
    registry = ConnectorRegistry()
    registry.register_connector("mock", connector)
    return registry


def test_successful_dispatch_and_raw_preservation(tmp_path) -> None:
    request = ConnectorRequest(
        provider="mock",
        operation="latest",
        preserve_raw=True,
        raw_store_destination=str(tmp_path),
        correlation_id="corr",
    )
    response = dispatch(request, registry=registry_with(MockConnector()))
    payload = response.to_dict()
    assert payload["status"] == "success"
    assert payload["raw_reference"]["sha256"]
    assert payload["lineage"]["raw_sha256"] == payload["raw_reference"]["sha256"]
    assert payload["normalized_rows"][0]["value"] == 1
    assert "corr" in payload["request_metadata"]["correlation_id"]


def test_dispatch_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ProviderNotFoundError):
        dispatch(ConnectorRequest(provider="missing", operation="latest"), registry=ConnectorRegistry())
    with pytest.raises(UnsupportedOperationError):
        dispatch(ConnectorRequest(provider="mock", operation="other"), registry=registry_with(MockConnector()))
    with pytest.raises(DispatchError):
        dispatch(ConnectorRequest(provider="mock", operation="latest"), registry=registry_with(InvalidRequestConnector()))
    with pytest.raises(ResponseValidationError):
        dispatch(ConnectorRequest(provider="mock", operation="latest"), registry=registry_with(InvalidResponseConnector()))
    with pytest.raises(DispatchError):
        dispatch(ConnectorRequest(provider="mock", operation="latest"), registry=registry_with(BadFetchConnector()))
    monkeypatch.delenv("MOCK_API_KEY", raising=False)
    with pytest.raises(CredentialError):
        dispatch(ConnectorRequest(provider="mock", operation="latest"), registry=registry_with(CredentialConnector()))

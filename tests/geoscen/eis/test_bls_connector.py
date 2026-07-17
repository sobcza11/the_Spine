from __future__ import annotations

import json
from pathlib import Path

import pytest

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, TimeoutPolicy, UpstreamResponse
from spine.jobs.geoscen.eis.credentials import Credential
from spine.jobs.geoscen.eis.dispatcher import dispatch
from spine.jobs.geoscen.eis.exceptions import (
    CredentialError,
    ProviderNotFoundError,
    RequestValidationError,
    ResponseValidationError,
    UnsupportedOperationError,
    UpstreamAuthenticationError,
    UpstreamRateLimitError,
    UpstreamRequestError,
    UpstreamResponseError,
)
from spine.jobs.geoscen.eis.http_client import GovernedHttpClient
from spine.jobs.geoscen.eis.registry import ConnectorRegistry
from spine.jobs.geoscen.eis.structure_context.connectors.bls import BLSConnector, register_bls_connector
from spine.jobs.geoscen.eis.structure_context.connectors.bls.parsing import MAX_SERIES_IDS

FIXTURES = Path(__file__).parent / "fixtures"
FAKE_KEY = "fake-bls-key-for-tests"


class FakeHttpClient:
    def __init__(self, payload: bytes | None = None) -> None:
        self.payload = payload or fixture_bytes("bls_success.json")
        self.calls = []

    def request(self, method, url, *, correlation_id, headers=None, json=None, timeout_policy=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "correlation_id": correlation_id,
                "headers": headers or {},
                "json": json or {},
                "timeout_policy": timeout_policy,
            },
        )
        return UpstreamResponse(
            url=url,
            method=method,
            status_code=200,
            headers={"content-type": "application/json"},
            content=self.payload,
            retrieved_at="2026-01-01T00:00:00Z",
        )


class StatusSession:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def request(self, *args, **kwargs):
        class Response:
            url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
            headers = {"content-type": "application/json"}
            content = b'{"status":"REQUEST_FAILED"}'
            elapsed = None

        response = Response()
        response.status_code = self.status_code
        return response


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def make_request(**overrides) -> ConnectorRequest:
    parameters = {
        "series_ids": ["CUUR0000SA0"],
        "start_year": "2024",
        "end_year": "2024",
    }
    parameters.update(overrides.pop("parameters", {}))
    return ConnectorRequest(
        provider="bls",
        operation=overrides.pop("operation", "timeseries"),
        parameters=parameters,
        correlation_id=overrides.pop("correlation_id", "corr-bls"),
        timeout_policy=overrides.pop("timeout_policy", TimeoutPolicy(connect_seconds=3, read_seconds=9)),
        **overrides,
    )


def test_registration_and_duplicate_rejection() -> None:
    registry = ConnectorRegistry()
    connector = register_bls_connector(registry)
    assert connector.provider == "bls"
    assert connector.supported_operations == ("timeseries",)
    assert registry.get_connector("BLS") is connector
    with pytest.raises(RequestValidationError):
        register_bls_connector(registry)
    with pytest.raises(ProviderNotFoundError):
        registry.get_connector("fred")


def test_credentials_required_and_secret_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ConnectorRegistry()
    register_bls_connector(registry)
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    with pytest.raises(CredentialError) as missing:
        dispatch(make_request(), registry=registry, http_client=FakeHttpClient())
    assert FAKE_KEY not in str(missing.value)
    monkeypatch.setenv("BLS_API_KEY", "   ")
    with pytest.raises(CredentialError):
        dispatch(make_request(), registry=registry, http_client=FakeHttpClient())

    monkeypatch.setenv("BLS_API_KEY", f" {FAKE_KEY} ")
    response = dispatch(make_request(), registry=registry, http_client=FakeHttpClient())
    serialized = json.dumps(response.to_dict(), sort_keys=True)
    assert FAKE_KEY not in serialized
    assert "registrationkey" not in serialized.lower()


def test_request_validation_rules() -> None:
    connector = BLSConnector()
    assert connector.validate_request(make_request()).valid
    assert connector.normalized_request(make_request(parameters={"series_ids": [" A ", "B", "A"]}))["series_ids"] == ["A", "B"]
    assert connector.validate_request(make_request(parameters={"series_ids": ["A", "B"]})).valid
    invalid_cases = [
        {"series_ids": []},
        {"series_ids": ["bad space"]},
        {"series_ids": ["A"] * (MAX_SERIES_IDS + 1)},
        {"start_year": "2025", "end_year": "2024"},
        {"endpoint": "https://evil.example"},
    ]
    for params in invalid_cases:
        assert not connector.validate_request(make_request(parameters=params)).valid
    with pytest.raises(UnsupportedOperationError):
        connector.build_request(make_request(operation="other"), {"BLS_API_KEY": Credential("BLS_API_KEY", FAKE_KEY)})


def test_request_construction_secret_boundary() -> None:
    connector = BLSConnector()
    request = make_request(parameters={"catalog": True, "annual_average": True})
    built = connector.build_request(request, {"BLS_API_KEY": Credential("BLS_API_KEY", FAKE_KEY)})
    assert built["method"] == "POST"
    assert built["url"] == "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    assert built["headers"]["Content-Type"] == "application/json"
    assert built["json"]["seriesid"] == ["CUUR0000SA0"]
    assert built["json"]["registrationkey"] == FAKE_KEY
    assert built["json"]["annualaverage"] is True
    assert "registration" not in json.dumps(built["safe_metadata"]).lower()
    assert FAKE_KEY not in json.dumps(built["safe_metadata"])


def test_fetch_uses_stage1_http_contract() -> None:
    connector = BLSConnector()
    fake = FakeHttpClient()
    request = make_request()
    connector.fetch(request, fake, {"BLS_API_KEY": Credential("BLS_API_KEY", FAKE_KEY)})
    call = fake.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == connector.endpoint
    assert call["headers"]["Content-Type"] == "application/json"
    assert call["correlation_id"] == "corr-bls"
    assert call["timeout_policy"].as_tuple() == (3, 9)
    assert call["json"]["registrationkey"] == FAKE_KEY


def test_response_parsing_success_periods_values_and_metadata() -> None:
    parsed = BLSConnector().parse_response(
        UpstreamResponse(
            url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
            method="POST",
            status_code=200,
            headers={"content-type": "application/json"},
            content=fixture_bytes("bls_success.json"),
            retrieved_at="2026-01-01T00:00:00Z",
        ),
        make_request(parameters={"series_ids": ["LNS14000000", "CUUR0000SA0"]}),
    )
    rows = parsed["normalized_rows"]
    assert [row["series_id"] for row in rows] == ["CUUR0000SA0", "CUUR0000SA0", "LNS14000000", "LNS14000000"]
    assert rows[0]["period"] == "M01"
    assert rows[0]["measurement_date"] == "2024-01-01"
    assert rows[1]["period"] == "M13"
    assert rows[1]["date_convention"] == "annual_average_no_specific_day"
    assert rows[2]["period"] == "A01"
    assert rows[3]["period"] == "Q01"
    assert rows[3]["value"] is None
    assert rows[1]["footnote_codes"] == ["P"]
    assert parsed["source_metadata"].provider == "bls"
    assert parsed["source_metadata"].measurement_as_of == "2024-Q01"
    assert parsed["validation"].valid


def test_partial_response_warnings_and_dispatch_status(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ConnectorRegistry()
    register_bls_connector(registry)
    monkeypatch.setenv("BLS_API_KEY", FAKE_KEY)
    response = dispatch(
        make_request(parameters={"series_ids": ["LNS14000000", "MISSING000"]}),
        registry=registry,
        http_client=FakeHttpClient(fixture_bytes("bls_partial_success.json")),
    )
    payload = response.to_dict()
    assert payload["status"] == "partial"
    assert len(payload["normalized_rows"]) == 1
    assert any("missing_series:MISSING000" == warning for warning in payload["warnings"])
    assert FAKE_KEY not in json.dumps(payload)


def test_error_and_malformed_response_handling() -> None:
    connector = BLSConnector()
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(_response(fixture_bytes("bls_error.json")), make_request())
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(_response(b"{not-json"), make_request())
    with pytest.raises(ResponseValidationError):
        connector.parse_response(_response(b'{"status":"REQUEST_SUCCEEDED"}'), make_request())
    with pytest.raises(ResponseValidationError):
        connector.parse_response(_response(b'{"status":"REQUEST_SUCCEEDED","Results":{}}'), make_request())


def test_http_status_mapping() -> None:
    client_401 = GovernedHttpClient(session=StatusSession(401))
    with pytest.raises(UpstreamAuthenticationError):
        client_401.request("POST", BLSConnector.endpoint, correlation_id="c", json={})
    client_429 = GovernedHttpClient(session=StatusSession(429))
    with pytest.raises(UpstreamRateLimitError):
        client_429.request("POST", BLSConnector.endpoint, correlation_id="c", json={})
    client_500 = GovernedHttpClient(session=StatusSession(500))
    with pytest.raises(UpstreamRequestError):
        client_500.request("POST", BLSConnector.endpoint, correlation_id="c", json={})


def test_raw_preservation_lineage_and_no_raw_body(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    registry = ConnectorRegistry()
    register_bls_connector(registry)
    monkeypatch.setenv("BLS_API_KEY", FAKE_KEY)
    without_raw = dispatch(make_request(), registry=registry, http_client=FakeHttpClient())
    assert without_raw.raw_reference is None
    with_raw = dispatch(
        make_request(preserve_raw=True, raw_store_destination=str(tmp_path)),
        registry=registry,
        http_client=FakeHttpClient(),
    )
    payload = with_raw.to_dict()
    assert payload["raw_reference"]["sha256"]
    assert payload["lineage"]["raw_sha256"] == payload["raw_reference"]["sha256"]
    serialized = json.dumps(payload, sort_keys=True)
    assert "Consumer Price Index for All Urban Consumers" in serialized
    assert '"status": "REQUEST_SUCCEEDED"' not in serialized
    assert FAKE_KEY not in serialized


def _response(content: bytes) -> UpstreamResponse:
    return UpstreamResponse(
        url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
        method="POST",
        status_code=200,
        headers={"content-type": "application/json"},
        content=content,
        retrieved_at="2026-01-01T00:00:00Z",
    )

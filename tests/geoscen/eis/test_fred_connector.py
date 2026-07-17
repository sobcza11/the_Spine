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
    UpstreamAuthenticationError,
    UpstreamRateLimitError,
    UpstreamRequestError,
    UpstreamResponseError,
)
from spine.jobs.geoscen.eis.http_client import GovernedHttpClient
from spine.jobs.geoscen.eis.registry import ConnectorRegistry
from spine.jobs.geoscen.eis.structure_context.connectors.fred import FREDConnector, register_fred_connector
from spine.jobs.geoscen.eis.structure_context.connectors.fred.parsing import MAX_LIMIT

FIXTURES = Path(__file__).parent / "fixtures"
FAKE_KEY = "fake-fred-key-for-tests"


class FakeHttpClient:
    def __init__(self, payload: bytes | None = None) -> None:
        self.payload = payload or fixture_bytes("fred_observations_success.json")
        self.calls = []

    def request(self, method, url, *, correlation_id, headers=None, params=None, timeout_policy=None, **kwargs):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "correlation_id": correlation_id,
                "headers": headers or {},
                "params": params or {},
                "timeout_policy": timeout_policy,
                "kwargs": kwargs,
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
            url = "https://api.stlouisfed.org/fred/series/observations?api_key=secret"
            headers = {"content-type": "application/json"}
            content = b'{"error_code":400,"error_message":"bad"}'
            elapsed = None

        response = Response()
        response.status_code = self.status_code
        return response


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def make_request(operation: str = "series_observations", **overrides) -> ConnectorRequest:
    parameters = {"series_id": "UNRATE"}
    parameters.update(overrides.pop("parameters", {}))
    return ConnectorRequest(
        provider="fred",
        operation=operation,
        parameters=parameters,
        correlation_id=overrides.pop("correlation_id", "corr-fred"),
        timeout_policy=overrides.pop("timeout_policy", TimeoutPolicy(connect_seconds=2, read_seconds=8)),
        **overrides,
    )


def test_registration_and_duplicate_rejection() -> None:
    registry = ConnectorRegistry()
    connector = register_fred_connector(registry)
    assert connector.provider == "fred"
    assert set(connector.supported_operations) == {"series_observations", "series_metadata"}
    assert registry.get_connector("FRED") is connector
    with pytest.raises(RequestValidationError):
        register_fred_connector(registry)
    with pytest.raises(ProviderNotFoundError):
        registry.get_connector("bea")


def test_credentials_required_and_secret_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ConnectorRegistry()
    register_fred_connector(registry)
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with pytest.raises(CredentialError):
        dispatch(make_request(), registry=registry, http_client=FakeHttpClient())
    monkeypatch.setenv("FRED_API_KEY", "   ")
    with pytest.raises(CredentialError):
        dispatch(make_request(), registry=registry, http_client=FakeHttpClient())
    monkeypatch.setenv("FRED_API_KEY", f" {FAKE_KEY} ")
    response = dispatch(make_request(), registry=registry, http_client=FakeHttpClient())
    serialized = json.dumps(response.to_dict(), sort_keys=True)
    assert FAKE_KEY not in serialized
    assert "api_key" not in serialized


def test_request_validation_rules() -> None:
    connector = FREDConnector()
    assert connector.validate_request(make_request()).valid
    assert connector.validate_request(make_request("series_metadata")).valid
    invalid_cases = [
        {"series_id": "https://evil.example"},
        {"series_id": "BAD?x=1"},
        {"observation_start": "2024/01/01"},
        {"observation_start": "2024-02-01", "observation_end": "2024-01-01"},
        {"frequency": "minute"},
        {"units": "weird"},
        {"aggregation_method": "median"},
        {"limit": str(MAX_LIMIT + 1)},
        {"offset": "-1"},
        {"endpoint": "https://evil.example"},
    ]
    for params in invalid_cases:
        assert not connector.validate_request(make_request(parameters=params)).valid
    assert not connector.validate_request(make_request("unsupported")).valid


def test_request_construction_and_fetch_secret_boundary() -> None:
    connector = FREDConnector()
    request = make_request(parameters={"observation_start": "2024-01-01", "limit": "10", "units": "lin"})
    built = connector.build_request(request, {"FRED_API_KEY": Credential("FRED_API_KEY", FAKE_KEY)})
    assert built["method"] == "GET"
    assert built["url"] == "https://api.stlouisfed.org/fred/series/observations"
    assert built["params"]["api_key"] == FAKE_KEY
    assert built["params"]["file_type"] == "json"
    assert list(built["params"]) == sorted(built["params"])
    assert FAKE_KEY not in json.dumps(built["safe_metadata"])
    assert "api_key" not in json.dumps(built["safe_metadata"])
    fake = FakeHttpClient()
    connector.fetch(request, fake, {"FRED_API_KEY": Credential("FRED_API_KEY", FAKE_KEY)})
    call = fake.calls[0]
    assert call["method"] == "GET"
    assert call["correlation_id"] == "corr-fred"
    assert call["timeout_policy"].as_tuple() == (2, 8)
    assert call["kwargs"] == {}


def test_observation_parsing_values_pagination_and_metadata() -> None:
    parsed = FREDConnector().parse_response(_response(fixture_bytes("fred_observations_success.json")), make_request())
    rows = parsed["normalized_rows"]
    assert [row["observation_date"] for row in rows] == ["2024-01-01", "2024-02-01", "2024-03-01"]
    assert rows[0]["value"] == 3.7
    assert rows[1]["value"] is None
    assert rows[1]["value_status"] == "malformed"
    assert rows[2]["raw_value"] == "."
    assert rows[2]["value_status"] == "unavailable"
    assert parsed["pagination"] == {"count": 5, "offset": 1, "limit": 3, "has_more": true_bool()}
    assert parsed["source_metadata"].provider == "fred"
    assert parsed["source_metadata"].measurement_as_of == "2024-01-01"
    assert any("malformed_numeric_value:2024-02-01" == warning for warning in parsed["warnings"])


def test_metadata_parsing_and_series_match() -> None:
    parsed = FREDConnector().parse_response(_response(fixture_bytes("fred_metadata_success.json")), make_request("series_metadata"))
    rows = parsed["normalized_rows"]
    assert rows[0]["series_id"] == "UNRATE"
    assert rows[0]["title"] == "Unemployment Rate"
    assert rows[0]["notes"].startswith("Long official notes")
    assert parsed["source_metadata"].publication_date == "2024-05-03 07:45:02-05"
    mismatch = json.loads(fixture_bytes("fred_metadata_success.json"))
    mismatch["seriess"][0]["id"] = "CPIAUCSL"
    with pytest.raises(ResponseValidationError):
        FREDConnector().parse_response(_response(json.dumps(mismatch).encode("utf-8")), make_request("series_metadata"))


def test_error_empty_and_malformed_response_handling() -> None:
    connector = FREDConnector()
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(_response(fixture_bytes("fred_error.json")), make_request())
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(_response(b"{not-json"), make_request())
    with pytest.raises(ResponseValidationError):
        connector.parse_response(_response(b'{"count":1}'), make_request())
    with pytest.raises(ResponseValidationError):
        connector.parse_response(_response(b'{"seriess":{}}'), make_request("series_metadata"))
    parsed = connector.parse_response(_response(fixture_bytes("fred_empty.json")), make_request())
    assert parsed["response_status"] == "unavailable"
    assert parsed["normalized_rows"] == []
    assert "no_observations_returned" in parsed["warnings"]


def test_http_status_mapping() -> None:
    for status, expected in [(401, UpstreamAuthenticationError), (403, UpstreamAuthenticationError), (429, UpstreamRateLimitError), (500, UpstreamRequestError)]:
        client = GovernedHttpClient(session=StatusSession(status))
        with pytest.raises(expected):
            client.request("GET", FREDConnector.endpoints["series_observations"], correlation_id="c", params={"api_key": FAKE_KEY})


def test_dispatch_raw_preservation_and_no_secret_leakage(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    registry = ConnectorRegistry()
    register_fred_connector(registry)
    monkeypatch.setenv("FRED_API_KEY", FAKE_KEY)
    response = dispatch(make_request(), registry=registry, http_client=FakeHttpClient())
    assert response.raw_reference is None
    with_raw = dispatch(
        make_request(preserve_raw=True, raw_store_destination=str(tmp_path)),
        registry=registry,
        http_client=FakeHttpClient(),
    )
    payload = with_raw.to_dict()
    assert payload["raw_reference"]["sha256"]
    assert payload["lineage"]["raw_sha256"] == payload["raw_reference"]["sha256"]
    serialized = json.dumps(payload, sort_keys=True)
    assert FAKE_KEY not in serialized
    assert '"observations"' not in serialized


def _response(content: bytes) -> UpstreamResponse:
    return UpstreamResponse(
        url="https://api.stlouisfed.org/fred/series/observations",
        method="GET",
        status_code=200,
        headers={"content-type": "application/json"},
        content=content,
        retrieved_at="2026-01-01T00:00:00Z",
    )


def true_bool() -> bool:
    return True

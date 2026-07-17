from __future__ import annotations

import json
from pathlib import Path

import pytest

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, TimeoutPolicy, UpstreamResponse
from spine.jobs.geoscen.eis.credentials import Credential, redact_mapping, redact_url
from spine.jobs.geoscen.eis.dispatcher import dispatch
from spine.jobs.geoscen.eis.exceptions import CredentialError, ProviderNotFoundError, RequestValidationError, ResponseValidationError, UpstreamAuthenticationError, UpstreamRateLimitError, UpstreamRequestError, UpstreamResponseError
from spine.jobs.geoscen.eis.http_client import GovernedHttpClient
from spine.jobs.geoscen.eis.registry import ConnectorRegistry
from spine.jobs.geoscen.eis.structure_context.connectors.bea import BEAConnector, register_bea_connector

FIXTURES = Path(__file__).parent / "fixtures"
FAKE_USER_ID = "12345678-1234-1234-1234-123456789abc"


class FakeHttpClient:
    def __init__(self, payload: bytes | None = None) -> None:
        self.payload = payload or fixture_bytes("bea_dataset_list_success.json")
        self.calls = []

    def request(self, method, url, *, correlation_id, headers=None, params=None, timeout_policy=None, **kwargs):
        self.calls.append({"method": method, "url": url, "correlation_id": correlation_id, "headers": headers or {}, "params": params or {}, "timeout_policy": timeout_policy, "kwargs": kwargs})
        return UpstreamResponse(url=url, method=method, status_code=200, headers={"content-type": "application/json"}, content=self.payload, retrieved_at="2026-01-01T00:00:00Z")


class StatusSession:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def request(self, *args, **kwargs):
        class Response:
            url = "https://apps.bea.gov/api/data?UserID=secret"
            headers = {"content-type": "application/json"}
            content = b'{"BEAAPI":{"Error":{"APIErrorCode":"1"}}}'
            elapsed = None
        response = Response()
        response.status_code = self.status_code
        return response


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def req(operation: str, parameters: dict | None = None, **kwargs) -> ConnectorRequest:
    return ConnectorRequest(provider="bea", operation=operation, parameters=parameters or {}, correlation_id=kwargs.pop("correlation_id", "corr-bea"), timeout_policy=kwargs.pop("timeout_policy", TimeoutPolicy(connect_seconds=4, read_seconds=10)), **kwargs)


def data_req(dataset_name: str = "NIPA", dataset_parameters: dict | None = None) -> ConnectorRequest:
    params = {"dataset_name": dataset_name, "dataset_parameters": dataset_parameters or {"TableName": "T10101", "Frequency": ["A", "Q"], "Year": ["2024", "2023"]}}
    return req("data", params)


def test_registration_and_duplicate_rejection() -> None:
    registry = ConnectorRegistry()
    connector = register_bea_connector(registry)
    assert connector.provider == "bea"
    assert set(connector.supported_operations) == {"dataset_list", "parameter_list", "parameter_values", "parameter_values_filtered", "data"}
    assert registry.get_connector("BEA") is connector
    with pytest.raises(RequestValidationError):
        register_bea_connector(registry)
    with pytest.raises(ProviderNotFoundError):
        registry.get_connector("hud")


def test_credentials_required_and_userid_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ConnectorRegistry()
    register_bea_connector(registry)
    monkeypatch.delenv("BEA_USER_ID", raising=False)
    with pytest.raises(CredentialError):
        dispatch(req("dataset_list"), registry=registry, http_client=FakeHttpClient())
    monkeypatch.setenv("BEA_USER_ID", "   ")
    with pytest.raises(CredentialError):
        dispatch(req("dataset_list"), registry=registry, http_client=FakeHttpClient())
    monkeypatch.setenv("BEA_USER_ID", FAKE_USER_ID)
    response = dispatch(req("dataset_list"), registry=registry, http_client=FakeHttpClient())
    assert FAKE_USER_ID not in json.dumps(response.to_dict(), sort_keys=True)
    assert redact_mapping({"UserID": FAKE_USER_ID})["UserID"] == "[redacted]"
    assert FAKE_USER_ID not in redact_url(f"https://apps.bea.gov/api/data?UserID={FAKE_USER_ID}")


def test_request_validation_metadata_and_data_rules() -> None:
    connector = BEAConnector()
    assert connector.validate_request(req("dataset_list")).valid
    assert connector.validate_request(req("parameter_list", {"dataset_name": "NIPA"})).valid
    assert connector.validate_request(req("parameter_values", {"dataset_name": "Regional", "parameter_name": "GeoFips"})).valid
    assert connector.validate_request(req("parameter_values_filtered", {"dataset_name": "Regional", "target_parameter": "GeoFips", "filters": {"TableName": "CAINC1"}})).valid
    assert connector.validate_request(data_req("NIPA")).valid
    assert connector.validate_request(data_req("Regional", {"TableName": "CAINC1", "LineCode": "10", "GeoFips": ["01001", "00000"], "Year": "2024"})).valid
    with pytest.raises(RequestValidationError):
        req("parameter_values_filtered", {"dataset_name": "Regional", "target_parameter": "GeoFips", "filters": {"__proto__": "x"}})
    invalid = [
        req("parameter_list", {}),
        data_req("GDPbyIndustry"),
        data_req("NIPA", {"TableID": "1", "Frequency": "A", "Year": "2024"}),
        data_req("NIPA", {"TableName": "T10101", "Frequency": "Z", "Year": "2024"}),
        data_req("NIPA", {"TableName": "T10101", "Frequency": "A", "Year": "ALL"}),
        data_req("Regional", {"TableName": "CAINC1", "LineCode": "bad", "GeoFips": "01001"}),
        data_req("Regional", {"TableName": "CAINC1", "LineCode": "10", "GeoFips": "bad text"}),
        req("dataset_list", {"endpoint": "https://evil"}),
        req("dataset_list", {"method": "GetData"}),
        req("dataset_list", {"UserID": FAKE_USER_ID}),
    ]
    for item in invalid:
        assert not connector.validate_request(item).valid


def test_request_construction_and_fetch_secret_boundary() -> None:
    connector = BEAConnector()
    request = data_req()
    built = connector.build_request(request, {"BEA_USER_ID": Credential("BEA_USER_ID", FAKE_USER_ID)})
    assert built["method"] == "GET"
    assert built["url"] == "https://apps.bea.gov/api/data"
    assert built["params"]["UserID"] == FAKE_USER_ID
    assert built["params"]["method"] == "GetData"
    assert built["params"]["ResultFormat"] == "JSON"
    assert list(built["params"]) == sorted(built["params"])
    assert FAKE_USER_ID not in json.dumps(built["safe_metadata"])
    fake = FakeHttpClient()
    connector.fetch(request, fake, {"BEA_USER_ID": Credential("BEA_USER_ID", FAKE_USER_ID)})
    call = fake.calls[0]
    assert call["method"] == "GET"
    assert call["correlation_id"] == "corr-bea"
    assert call["timeout_policy"].as_tuple() == (4, 10)
    assert call["params"]["UserID"] == FAKE_USER_ID


def test_metadata_parsing_operations() -> None:
    connector = BEAConnector()
    datasets = connector.parse_response(resp("bea_dataset_list_success.json"), req("dataset_list"))
    assert [row["dataset_name"] for row in datasets["normalized_rows"]] == ["NIPA", "Regional"]
    params = connector.parse_response(resp("bea_parameter_list_success.json"), req("parameter_list", {"dataset_name": "NIPA"}))
    assert params["normalized_rows"][0]["parameter_name"] == "TableName"
    assert any("unknown_boolean_flag" in warning for warning in params["warnings"])
    values = connector.parse_response(resp("bea_parameter_values_success.json"), req("parameter_values", {"dataset_name": "Regional", "parameter_name": "GeoFips"}))
    assert values["normalized_rows"][0]["value_key"] == "01000"
    filtered = connector.parse_response(resp("bea_parameter_values_filtered_success.json"), req("parameter_values_filtered", {"dataset_name": "Regional", "target_parameter": "GeoFips", "filters": {"TableName": "CAINC1"}}))
    assert filtered["normalized_rows"][0]["applied_filter_keys"] == ["TableName"]


def test_nipa_and_regional_data_parsing() -> None:
    connector = BEAConnector()
    nipa = connector.parse_response(resp("bea_nipa_data_success.json"), data_req("NIPA"))
    rows = nipa["normalized_rows"]
    assert rows[0]["value"] == 28000.1
    assert rows[0]["unit_multiplier"] == "9"
    assert rows[1]["value"] is None
    assert rows[1]["period_kind"] == "monthly"
    assert rows[-1]["record_type"] == "note"
    assert nipa["source_metadata"].measurement_as_of == "2024Q1"
    regional = connector.parse_response(resp("bea_regional_data_success.json"), data_req("Regional", {"TableName": "CAINC1", "LineCode": "10", "GeoFips": ["01001", "00000"], "Year": ["2023", "2024"]}))
    rrows = regional["normalized_rows"]
    assert rrows[0]["geo_fips"] == "00000"
    assert rrows[1]["geo_fips"] == "01001"
    assert rrows[0]["value"] is None
    assert rrows[1]["value"] == 1234


def test_error_empty_http_and_raw_preservation(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    connector = BEAConnector()
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(resp("bea_error.json"), req("dataset_list"))
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(raw_resp(b"{bad"), req("dataset_list"))
    with pytest.raises(ResponseValidationError):
        connector.parse_response(raw_resp(b'{"x":1}'), req("dataset_list"))
    empty = connector.parse_response(resp("bea_empty.json"), req("dataset_list"))
    assert empty["response_status"] == "unavailable"
    assert "no_datasets_returned" in empty["warnings"]
    for status, expected in [(401, UpstreamAuthenticationError), (403, UpstreamAuthenticationError), (429, UpstreamRateLimitError), (500, UpstreamRequestError)]:
        with pytest.raises(expected):
            GovernedHttpClient(session=StatusSession(status)).request("GET", BEAConnector.endpoint, correlation_id="c", params={"UserID": FAKE_USER_ID})
    registry = ConnectorRegistry()
    register_bea_connector(registry)
    monkeypatch.setenv("BEA_USER_ID", FAKE_USER_ID)
    response = dispatch(req("dataset_list", preserve_raw=True, raw_store_destination=str(tmp_path)), registry=registry, http_client=FakeHttpClient())
    payload = response.to_dict()
    assert payload["raw_reference"]["sha256"]
    assert payload["lineage"]["raw_sha256"] == payload["raw_reference"]["sha256"]
    assert FAKE_USER_ID not in json.dumps(payload, sort_keys=True)
    assert '"BEAAPI"' not in json.dumps(payload, sort_keys=True)


def resp(name: str) -> UpstreamResponse:
    return raw_resp(fixture_bytes(name))


def raw_resp(content: bytes) -> UpstreamResponse:
    return UpstreamResponse(url="https://apps.bea.gov/api/data", method="GET", status_code=200, headers={"content-type": "application/json"}, content=content, retrieved_at="2026-01-01T00:00:00Z")


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()

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
from spine.jobs.geoscen.eis.structure_context.connectors.census_acs import CensusACSConnector, register_census_acs_connector

FIXTURES = Path(__file__).parent / "fixtures"
FAKE_KEY = "fake-census-key-000000000000"


class FakeHttpClient:
    def __init__(self, payload: bytes | None = None) -> None:
        self.payload = payload or fixture_bytes("census_acs_variables_success.json")
        self.calls = []

    def request(self, method, url, *, correlation_id, headers=None, params=None, timeout_policy=None, **kwargs):
        self.calls.append({"method": method, "url": url, "correlation_id": correlation_id, "headers": headers or {}, "params": params or {}, "timeout_policy": timeout_policy, "kwargs": kwargs})
        return UpstreamResponse(url=url, method=method, status_code=200, headers={"content-type": "application/json"}, content=self.payload, retrieved_at="2026-01-01T00:00:00Z")


class StatusSession:
    def __init__(self, status_code: int, content: bytes = b"error") -> None:
        self.status_code = status_code
        self.content = content

    def request(self, *args, **kwargs):
        class Response:
            url = "https://api.census.gov/data/2023/acs/acs5?key=secret"
            headers = {"content-type": "text/plain"}
            elapsed = None
        response = Response()
        response.status_code = self.status_code
        response.content = self.content
        return response


def req(operation: str, parameters: dict | None = None, **kwargs) -> ConnectorRequest:
    return ConnectorRequest(provider="census_acs", operation=operation, parameters=parameters or {}, correlation_id=kwargs.pop("correlation_id", "corr-census"), timeout_policy=kwargs.pop("timeout_policy", TimeoutPolicy(connect_seconds=4, read_seconds=10)), **kwargs)


def base_params(**extra) -> dict:
    params = {"year": "2023", "product": "acs5"}
    params.update(extra)
    return params


def state_data_params() -> dict:
    return base_params(variables=["NAME", "B01001_001E", "B01001_001E"], geography={"for": {"type": "state", "value": "*"}})


def county_data_params() -> dict:
    return base_params(variables=["NAME", "B01001_001E"], geography={"for": {"type": "county", "value": "*"}, "in": [{"type": "state", "value": "26"}]})


def tract_data_params() -> dict:
    return base_params(variables=["NAME", "B01001_001E"], geography={"for": {"type": "tract", "value": "*"}, "in": [{"type": "state", "value": "26"}, {"type": "county", "value": "125"}]})


def block_group_params() -> dict:
    return base_params(variables=["NAME", "B01001_001E"], geography={"for": {"type": "block group", "value": "*"}, "in": [{"type": "state", "value": "26"}, {"type": "county", "value": "125"}, {"type": "tract", "value": "405100"}]})


def test_registration_and_duplicate_rejection() -> None:
    registry = ConnectorRegistry()
    connector = register_census_acs_connector(registry)
    assert connector.provider == "census_acs"
    assert set(connector.supported_operations) == {"variables", "groups", "group_variables", "geography", "data"}
    assert registry.get_connector("CENSUS_ACS") is connector
    with pytest.raises(RequestValidationError):
        register_census_acs_connector(registry)
    with pytest.raises(ProviderNotFoundError):
        registry.get_connector("fhfa")


def test_credentials_required_and_key_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ConnectorRegistry()
    register_census_acs_connector(registry)
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    with pytest.raises(CredentialError):
        dispatch(req("variables", base_params()), registry=registry, http_client=FakeHttpClient())
    monkeypatch.setenv("CENSUS_API_KEY", "   ")
    with pytest.raises(CredentialError):
        dispatch(req("variables", base_params()), registry=registry, http_client=FakeHttpClient())
    monkeypatch.setenv("CENSUS_API_KEY", FAKE_KEY)
    response = dispatch(req("variables", base_params()), registry=registry, http_client=FakeHttpClient())
    assert FAKE_KEY not in json.dumps(response.to_dict(), sort_keys=True)
    assert redact_mapping({"key": FAKE_KEY})["key"] == "[redacted]"
    assert FAKE_KEY not in redact_url(f"https://api.census.gov/data/2023/acs/acs5?key={FAKE_KEY}")


def test_product_year_metadata_and_data_validation() -> None:
    connector = CensusACSConnector()
    assert connector.validate_request(req("variables", base_params(product="acs5"))).valid
    assert connector.validate_request(req("variables", base_params(product="acs5/profile"))).valid
    assert connector.validate_request(req("variables", base_params(product="acs5/subject"))).valid
    assert connector.validate_request(req("groups", base_params())).valid
    assert connector.validate_request(req("group_variables", base_params(group="B01001"))).valid
    assert connector.validate_request(req("geography", base_params())).valid
    assert connector.validate_request(req("data", state_data_params())).valid
    assert connector.validate_request(req("data", county_data_params())).valid
    assert connector.validate_request(req("data", base_params(variables=["NAME"], geography={"for": {"type": "place", "value": "*"}, "in": [{"type": "state", "value": "26"}]}))).valid
    assert connector.validate_request(req("data", tract_data_params())).valid
    assert connector.validate_request(req("data", block_group_params())).valid
    with pytest.raises(RequestValidationError):
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "state", "value": "*"}}, predicates={"__proto__": "x"}))
    invalid = [
        req("variables", base_params(product="acs1")),
        req("variables", base_params(product="acs5/pums")),
        req("variables", base_params(year="3026")),
        req("variables", base_params(year="bad")),
        req("group_variables", base_params(group="../B01001")),
        req("data", base_params(variables=[], geography={"for": {"type": "state", "value": "*"}})),
        req("data", base_params(variables=["bad var"], geography={"for": {"type": "state", "value": "*"}})),
        req("data", base_params(variables=[f"B01001_{idx:03d}E" for idx in range(30)], geography={"for": {"type": "state", "value": "*"}})),
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "state", "value": "2"}})),
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "county", "value": "*"}})),
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "tract", "value": "*"}, "in": [{"type": "state", "value": "26"}]})),
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "block group", "value": "*"}, "in": [{"type": "state", "value": "26"}, {"type": "county", "value": "125"}]})),
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "tract", "value": "bad"}, "in": [{"type": "state", "value": "26"}, {"type": "county", "value": "125"}]})),
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "state", "value": "*"}}, key=FAKE_KEY)),
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "state", "value": "*"}}, endpoint="https://evil")),
        req("data", base_params(variables=["NAME"], geography={"for": {"type": "state", "value": "*"}}, predicates={"get": "NAME"})),
    ]
    for item in invalid:
        assert not connector.validate_request(item).valid


def test_request_construction_and_fetch_secret_boundary() -> None:
    connector = CensusACSConnector()
    request = req("data", state_data_params())
    built = connector.build_request(request, {"CENSUS_API_KEY": Credential("CENSUS_API_KEY", FAKE_KEY)})
    assert built["method"] == "GET"
    assert built["url"] == "https://api.census.gov/data/2023/acs/acs5"
    assert built["params"]["key"] == FAKE_KEY
    assert built["params"]["get"] == "NAME,B01001_001E"
    assert built["params"]["for"] == "state:*"
    assert list(built["params"]) == sorted(built["params"])
    assert FAKE_KEY not in json.dumps(built["safe_metadata"])
    group = connector.build_request(req("group_variables", base_params(product="acs5/profile", group="DP03")), {"CENSUS_API_KEY": Credential("CENSUS_API_KEY", FAKE_KEY)})
    assert group["url"].endswith("/2023/acs/acs5/profile/groups/DP03.json")
    fake = FakeHttpClient()
    connector.fetch(request, fake, {"CENSUS_API_KEY": Credential("CENSUS_API_KEY", FAKE_KEY)})
    call = fake.calls[0]
    assert call["correlation_id"] == "corr-census"
    assert call["timeout_policy"].as_tuple() == (4, 10)


def test_metadata_parsing_operations() -> None:
    connector = CensusACSConnector()
    variables = connector.parse_response(resp("census_acs_variables_success.json"), req("variables", base_params()))
    assert [row["variable"] for row in variables["normalized_rows"]] == ["B01001_001E", "NAME"]
    groups = connector.parse_response(resp("census_acs_groups_success.json"), req("groups", base_params()))
    assert groups["normalized_rows"][0]["group"] == "B01001"
    group_vars = connector.parse_response(resp("census_acs_group_variables_success.json"), req("group_variables", base_params(group="B01001")))
    assert group_vars["normalized_rows"][0]["concept"] == "Sex by Age"
    geography = connector.parse_response(resp("census_acs_geography_success.json"), req("geography", base_params()))
    assert geography["normalized_rows"][1]["geography_id"] == "state"
    empty = connector.parse_response(resp("census_acs_empty.json"), req("variables", base_params()))
    assert empty["response_status"] == "unavailable"
    assert "no_variables_returned" in empty["warnings"]


def test_data_parsing_values_and_geography() -> None:
    connector = CensusACSConnector()
    state = connector.parse_response(resp("census_acs_data_state_success.json"), req("data", state_data_params()))
    rows = state["normalized_rows"]
    assert rows[0]["state"] == "01"
    assert rows[0]["variables"]["B01001_001E"]["value"] == 5039877
    assert rows[0]["variables"]["DP03_0062PE"]["value"] == 12.3
    assert rows[1]["state"] == "26"
    assert rows[1]["variables"]["B01001_001E"]["value"] is None
    assert rows[1]["variables"]["B01001_001E"]["value_status"] == "unavailable"
    county = connector.parse_response(resp("census_acs_data_county_success.json"), req("data", county_data_params()))
    assert county["normalized_rows"][0]["county"] == "125"
    tract = connector.parse_response(resp("census_acs_data_tract_success.json"), req("data", tract_data_params()))
    assert tract["normalized_rows"][0]["tract"] == "405100"


def test_error_empty_partial_http_and_raw_preservation(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    connector = CensusACSConnector()
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(resp("census_acs_error.json"), req("variables", base_params()))
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(raw_resp(b"{bad"), req("variables", base_params()))
    with pytest.raises(ResponseValidationError):
        connector.parse_response(raw_resp(b'{"x":1}'), req("variables", base_params()))
    with pytest.raises(ResponseValidationError):
        connector.parse_response(raw_resp(b'[["NAME","NAME"],["A","B"]]'), req("data", state_data_params()))
    partial = connector.parse_response(raw_resp(b'[["NAME","B01001_001E","state"],["Alabama","10","01"],["bad"]]'), req("data", state_data_params()))
    assert partial["response_status"] == "partial"
    assert "row_width_mismatch" in partial["warnings"]
    header_only = connector.parse_response(raw_resp(b'[["NAME","B01001_001E","state"]]'), req("data", state_data_params()))
    assert header_only["response_status"] == "unavailable"
    assert "no_data_rows" in header_only["warnings"]
    for status, expected in [(401, UpstreamAuthenticationError), (403, UpstreamAuthenticationError), (429, UpstreamRateLimitError), (404, UpstreamRequestError), (500, UpstreamRequestError)]:
        with pytest.raises(expected):
            GovernedHttpClient(session=StatusSession(status)).request("GET", CensusACSConnector.endpoint_template, correlation_id="c", params={"key": FAKE_KEY})
    registry = ConnectorRegistry()
    register_census_acs_connector(registry)
    monkeypatch.setenv("CENSUS_API_KEY", FAKE_KEY)
    response = dispatch(req("variables", base_params(), preserve_raw=True, raw_store_destination=str(tmp_path)), registry=registry, http_client=FakeHttpClient())
    payload = response.to_dict()
    assert payload["raw_reference"]["sha256"]
    assert payload["lineage"]["raw_sha256"] == payload["raw_reference"]["sha256"]
    assert FAKE_KEY not in json.dumps(payload, sort_keys=True)
    assert '"variables": {' not in json.dumps(payload, sort_keys=True)


def resp(name: str) -> UpstreamResponse:
    return raw_resp(fixture_bytes(name))


def raw_resp(content: bytes) -> UpstreamResponse:
    return UpstreamResponse(url="https://api.census.gov/data/2023/acs/acs5", method="GET", status_code=200, headers={"content-type": "application/json"}, content=content, retrieved_at="2026-01-01T00:00:00Z")


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()

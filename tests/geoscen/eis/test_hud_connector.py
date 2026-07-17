from __future__ import annotations

import json
from pathlib import Path

import pytest

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, TimeoutPolicy, UpstreamResponse
from spine.jobs.geoscen.eis.credentials import Credential, redact_mapping
from spine.jobs.geoscen.eis.dispatcher import dispatch
from spine.jobs.geoscen.eis.exceptions import CredentialError, ProviderNotFoundError, RequestValidationError, ResponseValidationError, UpstreamAuthenticationError, UpstreamRateLimitError, UpstreamRequestError, UpstreamResponseError
from spine.jobs.geoscen.eis.http_client import GovernedHttpClient
from spine.jobs.geoscen.eis.registry import ConnectorRegistry
from spine.jobs.geoscen.eis.structure_context.connectors.hud import HUDConnector, register_hud_connector

FIXTURES = Path(__file__).parent / "fixtures"
FAKE_TOKEN = "fake-hud-token-000000000000"


class FakeHttpClient:
    def __init__(self, payload: bytes | None = None) -> None:
        self.payload = payload or fixture_bytes("hud_states_success.json")
        self.calls = []

    def request(self, method, url, *, correlation_id, headers=None, params=None, timeout_policy=None, **kwargs):
        self.calls.append({"method": method, "url": url, "correlation_id": correlation_id, "headers": headers or {}, "params": params or {}, "timeout_policy": timeout_policy, "kwargs": kwargs})
        return UpstreamResponse(url=url, method=method, status_code=200, headers={"content-type": "application/json"}, content=self.payload, retrieved_at="2026-01-01T00:00:00Z")


class StatusSession:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def request(self, *args, **kwargs):
        class Response:
            url = "https://www.huduser.gov/hudapi/public/fmr/listStates"
            headers = {"content-type": "application/json"}
            content = b'{"status":"error","message":"no"}'
            elapsed = None
        response = Response()
        response.status_code = self.status_code
        return response


def req(operation: str, parameters: dict | None = None, **kwargs) -> ConnectorRequest:
    return ConnectorRequest(provider="hud", operation=operation, parameters=parameters or {}, correlation_id=kwargs.pop("correlation_id", "corr-hud"), timeout_policy=kwargs.pop("timeout_policy", TimeoutPolicy(connect_seconds=4, read_seconds=10)), **kwargs)


def test_registration_credentials_and_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ConnectorRegistry()
    connector = register_hud_connector(registry)
    assert connector.provider == "hud"
    assert set(connector.supported_operations) == {"list_states", "list_counties", "list_metro_areas", "fmr_data", "fmr_state_data", "income_limits_data", "income_limits_state_data"}
    assert connector.credential_specification == {"HUD_USER_ACCESS_TOKEN": True}
    assert registry.get_connector("HUD") is connector
    with pytest.raises(RequestValidationError):
        register_hud_connector(registry)
    with pytest.raises(ProviderNotFoundError):
        registry.get_connector("missing")
    monkeypatch.delenv("HUD_USER_ACCESS_TOKEN", raising=False)
    with pytest.raises(CredentialError):
        dispatch(req("list_states"), registry=registry, http_client=FakeHttpClient())
    monkeypatch.setenv("HUD_USER_ACCESS_TOKEN", "   ")
    with pytest.raises(CredentialError):
        dispatch(req("list_states"), registry=registry, http_client=FakeHttpClient())
    monkeypatch.setenv("HUD_USER_ACCESS_TOKEN", FAKE_TOKEN)
    response = dispatch(req("list_states"), registry=registry, http_client=FakeHttpClient())
    assert FAKE_TOKEN not in json.dumps(response.to_dict(), sort_keys=True)
    assert redact_mapping({"Authorization": f"Bearer {FAKE_TOKEN}"})["Authorization"] == "[redacted]"


def test_request_validation_and_construction() -> None:
    connector = HUDConnector()
    assert connector.validate_request(req("list_states")).valid
    assert connector.validate_request(req("list_counties", {"state_id": "mi", "updated_year": "2025"})).valid
    assert connector.validate_request(req("list_metro_areas", {"updated_year": "2025"})).valid
    assert connector.validate_request(req("fmr_data", {"entity_id": "2612599999", "year": "2025"})).valid
    assert connector.validate_request(req("fmr_state_data", {"state_code": "mi", "year": "2025"})).valid
    assert connector.validate_request(req("income_limits_data", {"entity_id": "2612599999", "year": "2025"})).valid
    assert connector.validate_request(req("income_limits_state_data", {"state_code": "MI", "year": "2025"})).valid
    with pytest.raises(RequestValidationError):
        req("list_states", {"__proto__": "x"})
    invalid = [
        req("fmr_data", {"entity_id": "bad/id"}),
        req("fmr_data", {"entity_id": "https://evil"}),
        req("fmr_data", {"entity_id": "2612599999", "year": "bad"}),
        req("fmr_state_data", {"state_code": "XX"}),
        req("list_counties", {"state_id": "26"}),
        req("list_states", {"Authorization": "Bearer bad"}),
        req("list_states", {"endpoint": "https://evil"}),
        req("list_states", {"url": "https://evil"}),
        req("list_states", {"extra": "x"}),
    ]
    for item in invalid:
        assert not connector.validate_request(item).valid
    built = connector.build_request(req("fmr_data", {"entity_id": "2612599999", "year": "2025"}), {"HUD_USER_ACCESS_TOKEN": Credential("HUD_USER_ACCESS_TOKEN", FAKE_TOKEN)})
    assert built["method"] == "GET"
    assert built["url"] == "https://www.huduser.gov/hudapi/public/fmr/data/2612599999"
    assert built["params"] == {"year": "2025"}
    assert built["headers"]["Authorization"] == f"Bearer {FAKE_TOKEN}"
    assert FAKE_TOKEN not in json.dumps(built["safe_metadata"])
    fake = FakeHttpClient()
    connector.fetch(req("list_counties", {"state_id": "MI", "updated_year": "2025"}), fake, {"HUD_USER_ACCESS_TOKEN": Credential("HUD_USER_ACCESS_TOKEN", FAKE_TOKEN)})
    call = fake.calls[0]
    assert call["url"].endswith("/fmr/listCounties/MI")
    assert call["params"] == {"updated": "2025"}
    assert call["timeout_policy"].as_tuple() == (4, 10)


def test_lookup_parsing() -> None:
    connector = HUDConnector()
    states = connector.parse_response(resp("hud_states_success.json"), req("list_states"))
    assert [row["state_code"] for row in states["normalized_rows"]] == ["AL", "MI"]
    counties = connector.parse_response(resp("hud_counties_success.json"), req("list_counties", {"state_id": "MI"}))
    assert counties["normalized_rows"][0]["county_entity_id"] == "2612599999"
    assert counties["normalized_rows"][0]["county_code"] == "26125"
    metros = connector.parse_response(resp("hud_metros_success.json"), req("list_metro_areas"))
    assert metros["normalized_rows"][0]["metro_code"] == "19804"
    empty = connector.parse_response(resp("hud_empty.json"), req("list_states"))
    assert empty["response_status"] == "unavailable"
    assert "no_records" in empty["warnings"]


def test_fmr_and_income_limits_parsing() -> None:
    connector = HUDConnector()
    fmr = connector.parse_response(resp("hud_fmr_entity_success.json"), req("fmr_data", {"entity_id": "2612599999", "year": "2025"}))
    row = fmr["normalized_rows"][0]
    assert row["dataset"] == "fmr"
    assert row["two_bedroom"]["value"] == 1380
    assert row["three_bedroom"]["value"] is None
    assert row["year"] == "2025"
    state_fmr = connector.parse_response(resp("hud_fmr_state_success.json"), req("fmr_state_data", {"state_code": "MI", "year": "2025"}))
    assert state_fmr["response_status"] == "partial"
    il = connector.parse_response(resp("hud_income_limits_entity_success.json"), req("income_limits_data", {"entity_id": "2612599999", "year": "2025"}))
    rows = il["normalized_rows"]
    assert rows[0]["income_category"] == "very_low_income"
    assert rows[0]["household_size"] == "1"
    assert rows[0]["income_limit"]["value"] == 36500
    assert rows[2]["income_limit"]["value"] is None
    state_il = connector.parse_response(resp("hud_income_limits_state_success.json"), req("income_limits_state_data", {"state_code": "MI", "year": "2025"}))
    assert state_il["response_status"] == "partial"


def test_error_http_partial_raw_and_dispatch(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    connector = HUDConnector()
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(resp("hud_error.json"), req("list_states"))
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(raw_resp(b"{bad"), req("list_states"))
    with pytest.raises(UpstreamResponseError):
        connector.parse_response(raw_resp(b"<html>bad</html>", content_type="text/html"), req("list_states"))
    with pytest.raises(ResponseValidationError):
        connector.parse_response(raw_resp(b'[]'), req("list_states"))
    partial = connector.parse_response(resp("hud_partial.json"), req("fmr_state_data", {"state_code": "MI", "year": "2025"}))
    assert partial["response_status"] == "partial"
    assert "malformed_fmr_record" in partial["warnings"]
    for status, expected in [(401, UpstreamAuthenticationError), (403, UpstreamAuthenticationError), (429, UpstreamRateLimitError), (404, UpstreamRequestError), (500, UpstreamRequestError)]:
        with pytest.raises(expected):
            GovernedHttpClient(session=StatusSession(status)).request("GET", "https://www.huduser.gov/hudapi/public/fmr/listStates", correlation_id="c", headers={"Authorization": f"Bearer {FAKE_TOKEN}"})
    registry = ConnectorRegistry()
    register_hud_connector(registry)
    monkeypatch.setenv("HUD_USER_ACCESS_TOKEN", FAKE_TOKEN)
    response = dispatch(req("list_states", preserve_raw=True, raw_store_destination=str(tmp_path)), registry=registry, http_client=FakeHttpClient())
    payload = response.to_dict()
    assert payload["raw_reference"]["sha256"]
    assert payload["lineage"]["raw_sha256"] == payload["raw_reference"]["sha256"]
    assert FAKE_TOKEN not in json.dumps(payload, sort_keys=True)
    assert '"data":' not in json.dumps(payload, sort_keys=True)


def resp(name: str) -> UpstreamResponse:
    return raw_resp(fixture_bytes(name))


def raw_resp(content: bytes, *, content_type: str = "application/json") -> UpstreamResponse:
    return UpstreamResponse(url="https://www.huduser.gov/hudapi/public/fmr/listStates", method="GET", status_code=200, headers={"content-type": content_type}, content=content, retrieved_at="2026-01-01T00:00:00Z")


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()

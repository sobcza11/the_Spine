from __future__ import annotations

import json
from pathlib import Path

import pytest

from spine.jobs.geoscen.eis.contracts import ConnectorRequest, TimeoutPolicy, UpstreamResponse
from spine.jobs.geoscen.eis.dispatcher import dispatch
from spine.jobs.geoscen.eis.exceptions import ProviderNotFoundError, RequestValidationError, ResponseValidationError, UpstreamRateLimitError, UpstreamRequestError
from spine.jobs.geoscen.eis.http_client import GovernedHttpClient
from spine.jobs.geoscen.eis.registry import ConnectorRegistry
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa import FHFAConnector, register_fhfa_connector
from spine.jobs.geoscen.eis.structure_context.connectors.fhfa.datasets import DATASETS, FHFADataset, list_datasets

FIXTURES = Path(__file__).parent / "fixtures"


class FakeHttpClient:
    def __init__(self, payload: bytes | None = None, content_type: str = "text/csv") -> None:
        self.payload = payload or fixture_bytes("fhfa_hpi_national.csv")
        self.content_type = content_type
        self.calls = []

    def request(self, method, url, *, correlation_id, headers=None, params=None, timeout_policy=None, **kwargs):
        self.calls.append({"method": method, "url": url, "correlation_id": correlation_id, "headers": headers or {}, "params": params or {}, "timeout_policy": timeout_policy, "kwargs": kwargs})
        return UpstreamResponse(url=url, method=method, status_code=200, headers={"content-type": self.content_type}, content=self.payload, retrieved_at="2026-01-01T00:00:00Z")


class StatusSession:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def request(self, *args, **kwargs):
        class Response:
            url = "https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv"
            headers = {"content-type": "text/csv"}
            content = b""
            elapsed = None
        response = Response()
        response.status_code = self.status_code
        return response


def req(operation: str, parameters: dict | None = None, **kwargs) -> ConnectorRequest:
    return ConnectorRequest(provider="fhfa", operation=operation, parameters=parameters or {}, correlation_id=kwargs.pop("correlation_id", "corr-fhfa"), timeout_policy=kwargs.pop("timeout_policy", TimeoutPolicy(connect_seconds=4, read_seconds=10)), **kwargs)


def test_registration_catalog_and_no_credentials() -> None:
    registry = ConnectorRegistry()
    connector = register_fhfa_connector(registry)
    assert connector.provider == "fhfa"
    assert set(connector.supported_operations) == {"dataset_catalog", "hpi_download", "hpi_parse", "hpi_fetch"}
    assert connector.credential_specification == {}
    assert registry.get_connector("FHFA") is connector
    with pytest.raises(RequestValidationError):
        register_fhfa_connector(registry)
    with pytest.raises(ProviderNotFoundError):
        registry.get_connector("hud")
    ids = [dataset.dataset_id for dataset in list_datasets()]
    assert ids == sorted(ids)
    assert len(ids) == len(set(ids))
    assert DATASETS["hpi_national_purchase_only"].source_url.startswith("https://www.fhfa.gov/hpi/download/")


def test_request_validation_and_filters() -> None:
    connector = FHFAConnector()
    assert connector.validate_request(req("dataset_catalog")).valid
    assert connector.validate_request(req("dataset_catalog", {"geography_level": "state", "index_type": "all-transactions", "frequency": "quarterly"})).valid
    assert connector.validate_request(req("hpi_download", {"dataset_id": "hpi_national_purchase_only"})).valid
    assert connector.validate_request(req("hpi_fetch", {"dataset_id": "hpi_state_all_transactions"})).valid
    assert connector.validate_request(req("hpi_parse", {"dataset_id": "hpi_county_all_transactions", "raw_content": fixture_text("fhfa_hpi_county.csv")})).valid
    invalid = [
        req("hpi_download", {"dataset_id": "__proto__"}),
        req("hpi_download", {"dataset_id": "missing"}),
        req("hpi_download", {"dataset_id": "hpi_national_purchase_only", "url": "https://evil"}),
        req("hpi_download", {"dataset_id": "hpi_national_purchase_only", "endpoint": "https://evil"}),
        req("hpi_parse", {"dataset_id": "hpi_national_purchase_only", "path": "C:/tmp/file.csv"}),
        req("hpi_parse", {"dataset_id": "hpi_national_purchase_only"}),
        req("dataset_catalog", {"active_only": "yes"}),
        req("dataset_catalog", {"geography_level": "../state"}),
    ]
    for item in invalid:
        assert not connector.validate_request(item).valid


def test_request_construction_download_and_dispatch_raw(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    connector = FHFAConnector()
    request = req("hpi_fetch", {"dataset_id": "hpi_national_purchase_only"})
    built = connector.build_request(request, {})
    assert built["method"] == "GET"
    assert built["url"] == DATASETS["hpi_national_purchase_only"].source_url
    assert built["params"] == {}
    assert built["safe_metadata"]["dataset_id"] == "hpi_national_purchase_only"
    fake = FakeHttpClient()
    connector.fetch(request, fake, {})
    call = fake.calls[0]
    assert call["method"] == "GET"
    assert call["correlation_id"] == "corr-fhfa"
    assert call["timeout_policy"].as_tuple() == (4, 10)
    registry = ConnectorRegistry()
    register_fhfa_connector(registry)
    response = dispatch(req("hpi_fetch", {"dataset_id": "hpi_national_purchase_only"}, preserve_raw=True, raw_store_destination=str(tmp_path)), registry=registry, http_client=FakeHttpClient())
    payload = response.to_dict()
    assert payload["raw_reference"]["sha256"]
    assert payload["lineage"]["raw_sha256"] == payload["raw_reference"]["sha256"]
    assert "United States,USA" not in json.dumps(payload)


def test_catalog_download_and_parsers() -> None:
    connector = FHFAConnector()
    catalog = connector.parse_response(raw_resp(b"[]", content_type="application/json", url="fhfa://dataset_catalog"), req("dataset_catalog"))
    assert catalog["response_status"] == "unavailable"
    registry = ConnectorRegistry()
    register_fhfa_connector(registry)
    response = dispatch(req("dataset_catalog", {"geography_level": "state"}), registry=registry, http_client=FakeHttpClient())
    assert response.normalized_rows[0]["dataset_id"] == "hpi_state_all_transactions"
    download = connector.parse_response(raw_resp(fixture_bytes("fhfa_hpi_national.csv")), req("hpi_download", {"dataset_id": "hpi_national_purchase_only"}))
    assert download["normalized_rows"][0]["size_bytes"] > 0
    national = connector.parse_response(raw_resp(fixture_bytes("fhfa_hpi_national.csv")), req("hpi_parse", {"dataset_id": "hpi_national_purchase_only", "raw_content": fixture_text("fhfa_hpi_national.csv")}))
    rows = national["normalized_rows"]
    assert rows[0]["geography_code"] == "US"
    assert rows[0]["period"] == "2024-12"
    assert rows[0]["index_value"] == 425.1
    assert rows[1]["index_value"] is None
    state = connector.parse_response(raw_resp(fixture_bytes("fhfa_hpi_state.csv")), req("hpi_parse", {"dataset_id": "hpi_state_all_transactions", "raw_content": fixture_text("fhfa_hpi_state.csv")}))
    assert state["normalized_rows"][0]["state_code"] == "MI"
    assert state["normalized_rows"][0]["period"] == "2024Q4"
    metro = connector.parse_response(raw_resp(fixture_bytes("fhfa_hpi_metro.csv")), req("hpi_parse", {"dataset_id": "hpi_metro_all_transactions", "raw_content": fixture_text("fhfa_hpi_metro.csv")}))
    assert metro["normalized_rows"][0]["metro_code"] == "19804"
    county = connector.parse_response(raw_resp(fixture_bytes("fhfa_hpi_county.csv")), req("hpi_parse", {"dataset_id": "hpi_county_all_transactions", "raw_content": fixture_text("fhfa_hpi_county.csv")}))
    assert county["normalized_rows"][0]["county_fips"] == "26125"
    assert county["source_metadata"].measurement_as_of == "2024"


def test_content_csv_error_and_partial_behaviors() -> None:
    connector = FHFAConnector()
    with pytest.raises(ResponseValidationError):
        connector.parse_response(raw_resp(fixture_bytes("fhfa_error.html"), content_type="text/html"), req("hpi_fetch", {"dataset_id": "hpi_national_purchase_only"}))
    with pytest.raises(ResponseValidationError):
        connector.parse_response(raw_resp(b""), req("hpi_fetch", {"dataset_id": "hpi_national_purchase_only"}))
    with pytest.raises(ResponseValidationError):
        connector.parse_response(raw_resp(fixture_bytes("fhfa_malformed.csv")), req("hpi_fetch", {"dataset_id": "hpi_national_purchase_only"}))
    empty = connector.parse_response(raw_resp(fixture_bytes("fhfa_hpi_empty.csv")), req("hpi_fetch", {"dataset_id": "hpi_national_purchase_only"}))
    assert empty["response_status"] == "unavailable"
    assert "no_data_rows" in empty["warnings"]
    partial_csv = b"county_name,county_fips,yr,index_nsa\nBad,bad,2024,1.2\nOakland County,26125,2024,288.5\n"
    partial = connector.parse_response(raw_resp(partial_csv), req("hpi_fetch", {"dataset_id": "hpi_county_all_transactions"}))
    assert partial["response_status"] == "partial"
    assert "invalid_required_row" in partial["warnings"]
    assert partial["normalized_rows"][0]["county_fips"] == "26125"
    too_big = DATASETS["hpi_national_purchase_only"]
    oversized = FHFADataset(**{**too_big.to_dict(), "maximum_expected_size": 5})
    from spine.jobs.geoscen.eis.structure_context.connectors.fhfa.downloads import validate_download_response
    with pytest.raises(ResponseValidationError):
        validate_download_response(raw_resp(fixture_bytes("fhfa_hpi_national.csv")), oversized)


def test_http_status_regressions() -> None:
    for status, expected in [(429, UpstreamRateLimitError), (404, UpstreamRequestError), (500, UpstreamRequestError)]:
        with pytest.raises(expected):
            GovernedHttpClient(session=StatusSession(status)).request("GET", DATASETS["hpi_national_purchase_only"].source_url, correlation_id="c")


def raw_resp(content: bytes, *, content_type: str = "text/csv", url: str | None = None) -> UpstreamResponse:
    return UpstreamResponse(url=url or DATASETS["hpi_national_purchase_only"].source_url, method="GET", status_code=200, headers={"content-type": content_type}, content=content, retrieved_at="2026-01-01T00:00:00Z")


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def fixture_text(name: str) -> str:
    return (FIXTURES / name).read_text()

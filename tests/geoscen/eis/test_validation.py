from __future__ import annotations

from spine.jobs.geoscen.eis.contracts import SCHEMA_VERSION
from spine.jobs.geoscen.eis.validation import (
    validate_content_type,
    validate_endpoint_safety,
    validate_normalized_rows,
    validate_numeric,
    validate_operation_id,
    validate_provider_id,
    validate_required_keys,
    validate_required_row_fields,
    validate_schema_version,
    validate_timestamp,
)


def test_required_keys_and_rows() -> None:
    assert validate_required_keys({"a": 1}, ["a"]).valid
    assert not validate_required_keys({"__proto__": 1}, ["a"]).valid
    rows = [{"date": "2026-01-01", "value": 1}]
    assert validate_required_row_fields(rows, ["date", "value"]).valid
    assert not validate_required_row_fields(rows, ["missing"]).valid


def test_timestamps_content_type_and_ids() -> None:
    assert validate_timestamp("2026-01-01T00:00:00Z").valid
    assert not validate_timestamp("01/01/2026").valid
    assert validate_content_type("application/json; charset=utf-8", ["application/json"]).valid
    assert not validate_content_type("text/html", ["application/json"]).valid
    assert validate_provider_id("census_acs").valid
    assert not validate_provider_id("../bad").valid
    assert validate_operation_id("latest").valid


def test_numeric_schema_endpoint_and_normalized_rows() -> None:
    assert validate_numeric("1.2").valid
    assert not validate_numeric(True).valid
    assert validate_schema_version(SCHEMA_VERSION).valid
    assert not validate_schema_version("bad").valid
    assert validate_endpoint_safety("https://api.example.com/data", ["api.example.com"]).valid
    assert not validate_endpoint_safety("http://api.example.com/data").valid
    assert validate_normalized_rows([{"a": 1}]).valid
    assert not validate_normalized_rows([{"constructor": "bad"}]).valid
    assert not validate_normalized_rows("bad").valid

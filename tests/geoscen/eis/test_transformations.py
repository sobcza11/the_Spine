from __future__ import annotations

from spine.jobs.geoscen.eis.transformations import latest_valid, parse_numeric, percent_change


def test_parse_numeric_propagates_provider_suppression() -> None:
    value, status, warnings = parse_numeric("-666666666")
    assert value is None
    assert status == "unavailable"
    assert "provider_suppression_code" in warnings


def test_latest_valid_skips_missing_values() -> None:
    result = latest_valid([{"period": "2024-01", "value": None}, {"period": "2024-02", "value": "5"}], value_field="value")
    assert result["value"] == 5.0


def test_percent_change_requires_two_valid_values() -> None:
    result = percent_change([{"period": "2024-01", "value": "10"}], value_field="value")
    assert result["status"] == "unavailable"
    assert "insufficient_history" in result["warnings"]

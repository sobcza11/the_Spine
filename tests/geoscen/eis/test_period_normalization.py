from __future__ import annotations

from spine.jobs.geoscen.eis.period_normalization import normalize_period


def test_monthly_period_has_deterministic_bounds() -> None:
    result = normalize_period({"measurement_period": "2024-02"}, frequency="monthly")
    assert result["period"] == "2024-02"
    assert result["period_end"] == "2024-02-29"


def test_quarterly_period_does_not_invent_specific_day() -> None:
    result = normalize_period({"year": "2024", "period": "Q01"}, frequency="quarterly")
    assert result["period"] == "2024Q01"
    assert result["period_start"] is None
    assert result["period_end"] is None

from __future__ import annotations

from spine.jobs.geoscen.eis.geography_normalization import normalize_geography


def test_normalize_state_geography_from_census_row() -> None:
    result = normalize_geography({"state": "26", "NAME": "Michigan"}, provider="census_acs", expected_level="state")
    assert result["geography_id"] == "state:26"
    assert result["geography_name"] == "Michigan"


def test_normalize_country_geography() -> None:
    result = normalize_geography({}, provider="bls", expected_level="country")
    assert result["geography_id"] == "country:US"
    assert result["country_code"] == "US"

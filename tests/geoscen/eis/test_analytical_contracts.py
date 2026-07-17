from __future__ import annotations

import pytest

from spine.jobs.geoscen.eis.analytical_contracts import AnalyticalContractError, AnalyticalObservation, FreshnessPolicy


def test_freshness_policy_requires_ordered_windows() -> None:
    FreshnessPolicy("monthly", 1, 2, 3)
    with pytest.raises(AnalyticalContractError):
        FreshnessPolicy("monthly", 3, 2, 1)


def test_observation_contract_is_json_safe() -> None:
    row = AnalyticalObservation(
        indicator_id="unemployment_rate",
        display_name="Unemployment Rate",
        family="labor",
        classification="context",
        geography={"geography_id": "country:US", "geography_level": "country"},
        period={"period": "2024-01", "frequency": "monthly", "measurement_as_of": None, "publication_date": None, "retrieval_timestamp": None},
        value=4.0,
        raw_value="4",
        unit="percent",
        status="measured",
        freshness="current",
        direction="lower_is_improving",
        source={"provider": "bls"},
        source_specification_id="bls_national_labor_unemployment",
        provider="bls",
        transformation="latest_valid",
        derived=False,
        input_references=(),
        evidence_ids=("eis_ev_test",),
        validation={"valid": True},
        lineage_reference={},
        measurement_as_of=None,
        publication_date=None,
        retrieval_timestamp=None,
        completion_status="complete",
    )
    assert row.to_dict()["value"] == 4.0

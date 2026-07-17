from __future__ import annotations

import pytest

from spine.jobs.geoscen.eis.indicator_specifications import IndicatorSpecification, IndicatorSpecificationError, initial_indicator_specifications, validate_indicator_specifications


def test_initial_indicator_specifications_are_canonical_stage9_set() -> None:
    specs = initial_indicator_specifications()
    validate_indicator_specifications(specs)
    assert len(specs) == 12
    assert {spec.provider for spec in specs} == {"bls", "census_acs", "fhfa", "hud", "bea"}
    assert "unemployment_rate" in {spec.indicator_id for spec in specs}
    assert {spec.classification for spec in specs} == {"structure", "context"}


def test_indicator_specifications_reject_executable_parameters() -> None:
    base = initial_indicator_specifications()[0]
    with pytest.raises(IndicatorSpecificationError):
        IndicatorSpecification(**{**base.to_dict(), "parameters": {"bad": lambda: None}})

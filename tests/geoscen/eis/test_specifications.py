from __future__ import annotations

import pytest

from spine.jobs.geoscen.eis.contracts import SCHEMA_VERSION
from spine.jobs.geoscen.eis.specifications import EISDatasetSpecification, EISSpecificationError, initial_stage8_specifications, validate_specifications


def test_initial_specifications_are_small_declarative_and_unique() -> None:
    specs = initial_stage8_specifications()
    assert [spec.provider for spec in specs] == ["bls", "fred", "bea", "census_acs", "fhfa", "hud"]
    assert all(spec.expected_schema_version == SCHEMA_VERSION for spec in specs)
    assert all("://" not in str(spec.parameters) for spec in specs)
    assert specs[4].credential_requirement == {}
    with pytest.raises(TypeError):
        specs[0].parameters["x"] = "y"  # type: ignore[index]
    with pytest.raises(EISSpecificationError):
        validate_specifications([specs[0], specs[0]])


def test_invalid_specifications_reject_provider_url_and_schema() -> None:
    base = initial_stage8_specifications()[0].to_dict()
    base["provider"] = "evil"
    with pytest.raises(EISSpecificationError):
        EISDatasetSpecification(**base)
    base = initial_stage8_specifications()[0].to_dict()
    base["parameters"] = {"url": "https://evil.example"}
    with pytest.raises(EISSpecificationError):
        EISDatasetSpecification(**base)
    base = initial_stage8_specifications()[0].to_dict()
    base["expected_schema_version"] = "bad"
    with pytest.raises(EISSpecificationError):
        EISDatasetSpecification(**base)

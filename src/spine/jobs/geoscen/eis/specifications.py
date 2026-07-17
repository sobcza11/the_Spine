from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from spine.jobs.geoscen.eis.contracts import SCHEMA_VERSION, _reject_unsafe

INTEGRATION_SCHEMA_VERSION = "geoscen.eis.integration.v1"
ALLOWED_PROVIDERS = {"bls", "fred", "bea", "census_acs", "fhfa", "hud"}


class EISSpecificationError(ValueError):
    pass


@dataclass(frozen=True)
class EISDatasetSpecification:
    specification_id: str
    provider: str
    operation: str
    purpose: str
    domain: str
    parameters: Mapping[str, Any]
    credential_requirement: Mapping[str, bool]
    preserve_raw: bool
    raw_destination_policy: str
    expected_schema_version: str
    transformation_version: str
    expected_frequency: str
    geography_level: str
    measurement_concept: str
    source_priority: int
    active: bool
    required: bool
    maximum_row_expectation: int
    owner: str
    notes: str = ""
    fixture_name: str | None = None

    def __post_init__(self) -> None:
        _reject_unsafe(self.parameters)
        if not _safe_slug(self.specification_id):
            raise EISSpecificationError("specification_id invalid")
        if self.provider not in ALLOWED_PROVIDERS:
            raise EISSpecificationError("provider invalid")
        if self.expected_schema_version != SCHEMA_VERSION:
            raise EISSpecificationError("schema version mismatch")
        if self.maximum_row_expectation < 0:
            raise EISSpecificationError("maximum_row_expectation invalid")
        if _contains_url(self.parameters):
            raise EISSpecificationError("arbitrary URL rejected")
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        object.__setattr__(self, "credential_requirement", MappingProxyType(dict(self.credential_requirement)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "specification_id": self.specification_id,
            "provider": self.provider,
            "operation": self.operation,
            "purpose": self.purpose,
            "domain": self.domain,
            "parameters": dict(self.parameters),
            "credential_requirement": dict(self.credential_requirement),
            "preserve_raw": self.preserve_raw,
            "raw_destination_policy": self.raw_destination_policy,
            "expected_schema_version": self.expected_schema_version,
            "transformation_version": self.transformation_version,
            "expected_frequency": self.expected_frequency,
            "geography_level": self.geography_level,
            "measurement_concept": self.measurement_concept,
            "source_priority": self.source_priority,
            "active": self.active,
            "required": self.required,
            "maximum_row_expectation": self.maximum_row_expectation,
            "owner": self.owner,
            "notes": self.notes,
            "fixture_name": self.fixture_name,
        }


def validate_specifications(specifications: list[EISDatasetSpecification]) -> None:
    seen: set[str] = set()
    for spec in specifications:
        if spec.specification_id in seen:
            raise EISSpecificationError("duplicate specification_id")
        seen.add(spec.specification_id)


def initial_stage8_specifications() -> tuple[EISDatasetSpecification, ...]:
    specs = (
        EISDatasetSpecification("bls_national_labor_unemployment", "bls", "timeseries", "Representative national labor series", "labor", {"series_ids": ["LNS14000000"], "start_year": "2024", "end_year": "2024"}, {"BLS_API_KEY": True}, False, "disabled", SCHEMA_VERSION, "bls-timeseries-v1", "monthly", "national", "unemployment rate", 1, True, True, 100, "GeoScen EIS", fixture_name="bls_success.json"),
        EISDatasetSpecification("fred_housing_starts", "fred", "series_observations", "Representative national housing macro series", "housing", {"series_id": "HOUST", "observation_start": "2024-01-01", "observation_end": "2024-12-31", "limit": 12}, {"FRED_API_KEY": True}, False, "disabled", SCHEMA_VERSION, "fred-series-observations-v1", "monthly", "national", "housing starts", 1, True, True, 100, "GeoScen EIS", fixture_name="fred_observations_success.json"),
        EISDatasetSpecification("bea_regional_metadata", "bea", "parameter_list", "Representative BEA Regional metadata discovery", "income", {"dataset_name": "Regional"}, {"BEA_USER_ID": True}, False, "disabled", SCHEMA_VERSION, "bea-parameter-list-v1", "metadata", "regional", "BEA Regional parameters", 2, True, False, 100, "GeoScen EIS", fixture_name="bea_parameter_list_success.json"),
        EISDatasetSpecification("census_acs_state_population", "census_acs", "data", "Representative ACS 5-year state request", "demographics", {"year": "2023", "product": "acs5", "variables": ["NAME", "B01001_001E"], "geography": {"for": {"type": "state", "value": "*"}}}, {"CENSUS_API_KEY": True}, False, "disabled", SCHEMA_VERSION, "census-acs-data-v1", "annual", "state", "population", 1, True, True, 100, "GeoScen EIS", fixture_name="census_acs_data_state_success.json"),
        EISDatasetSpecification("fhfa_hpi_national", "fhfa", "hpi_fetch", "Representative FHFA national HPI dataset", "housing", {"dataset_id": "hpi_national_purchase_only"}, {}, False, "disabled", SCHEMA_VERSION, "fhfa-hpi-national-v1", "monthly", "national", "house price index", 1, True, True, 1000, "GeoScen EIS", fixture_name="fhfa_hpi_national.csv"),
        EISDatasetSpecification("hud_state_lookup", "hud", "list_states", "Representative HUD geography lookup", "housing", {}, {"HUD_USER_ACCESS_TOKEN": True}, False, "disabled", SCHEMA_VERSION, "hud-state-list-v1", "lookup", "state", "HUD state lookup", 2, True, False, 100, "GeoScen EIS", fixture_name="hud_states_success.json"),
    )
    validate_specifications(list(specs))
    return specs


def _safe_slug(value: str) -> bool:
    return bool(value) and all(ch.isalnum() or ch in {"_", "-"} for ch in value) and ".." not in value


def _contains_url(value: Any) -> bool:
    if isinstance(value, str):
        return "://" in value
    if isinstance(value, Mapping):
        return any(_contains_url(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_url(v) for v in value)
    return False

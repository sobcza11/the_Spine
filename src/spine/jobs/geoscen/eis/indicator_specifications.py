from __future__ import annotations

from dataclasses import asdict, dataclass, field
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from spine.jobs.geoscen.eis.analytical_contracts import (
    ALLOWED_CLASSIFICATIONS,
    ALLOWED_DIRECTIONS,
    ALLOWED_FAMILIES,
    ALLOWED_FREQUENCIES,
    ALLOWED_GEOGRAPHY_LEVELS,
    ANALYTICAL_SCHEMA_VERSION,
    FreshnessPolicy,
)
from spine.jobs.geoscen.eis.contracts import _reject_unsafe, to_json_safe
from spine.jobs.geoscen.eis.exceptions import RequestValidationError

TRANSFORMATION_REGISTRY = {
    "identity": "Return the governed source value unchanged.",
    "parse_numeric": "Parse a source scalar as numeric while preserving missing/suppressed states.",
    "percent_change": "Compute deterministic percent change from prior comparable value.",
    "absolute_change": "Compute deterministic absolute change from prior comparable value.",
    "basis_point_change": "Compute deterministic basis-point change from prior comparable rate.",
    "index_change": "Compute deterministic index-point change from prior comparable value.",
    "latest_valid": "Select latest comparable non-missing source value.",
    "prior_period_valid": "Select prior comparable non-missing source value.",
    "scale_by_unit_multiplier": "Scale a numeric value by a declared multiplier.",
    "wide_to_long": "Convert declared wide fields to long rows.",
    "select_household_size": "Select rows matching a declared household size.",
    "select_bedroom_count": "Select rows matching a declared bedroom count.",
}

VALID_UNITS = {"percent", "persons", "thousands", "usd", "usd_per_month", "index", "millions_2017_dollars"}


class IndicatorSpecificationError(ValueError):
    pass


@dataclass(frozen=True)
class IndicatorSpecification:
    indicator_id: str
    display_name: str
    family: str
    classification: str
    purpose: str
    provider: str
    source_specification_id: str
    source_operation: str
    source_fields: Sequence[str]
    geography_level: str
    frequency: str
    unit: str
    transformation: str
    direction_semantics: str
    required_inputs: Sequence[str]
    optional_inputs: Sequence[str]
    freshness_policy: FreshnessPolicy
    missing_policy: str
    validation_rules: Sequence[str]
    evidence_template: str
    schema_version: str = ANALYTICAL_SCHEMA_VERSION
    transformation_version: str = "geoscen-eis-stage9-v1"
    active: bool = True
    owner: str = "GeoScen EIS"
    notes: str = ""
    parameters: Mapping[str, Any] = field(default_factory=dict)
    fallback_source_specification_ids: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        try:
            _reject_unsafe(asdict(self))
        except RequestValidationError as exc:
            raise IndicatorSpecificationError(str(exc)) from exc
        if not _safe_slug(self.indicator_id):
            raise IndicatorSpecificationError("indicator_id invalid")
        if self.family not in ALLOWED_FAMILIES:
            raise IndicatorSpecificationError("family invalid")
        if self.classification not in ALLOWED_CLASSIFICATIONS:
            raise IndicatorSpecificationError("classification invalid")
        if self.transformation not in TRANSFORMATION_REGISTRY:
            raise IndicatorSpecificationError("transformation invalid")
        if self.frequency not in ALLOWED_FREQUENCIES:
            raise IndicatorSpecificationError("frequency invalid")
        if self.geography_level not in ALLOWED_GEOGRAPHY_LEVELS:
            raise IndicatorSpecificationError("geography_level invalid")
        if self.direction_semantics not in ALLOWED_DIRECTIONS:
            raise IndicatorSpecificationError("direction_semantics invalid")
        if self.unit not in VALID_UNITS:
            raise IndicatorSpecificationError("unit invalid")
        if self.schema_version != ANALYTICAL_SCHEMA_VERSION:
            raise IndicatorSpecificationError("schema_version invalid")
        object.__setattr__(self, "source_fields", tuple(self.source_fields))
        object.__setattr__(self, "required_inputs", tuple(self.required_inputs))
        object.__setattr__(self, "optional_inputs", tuple(self.optional_inputs))
        object.__setattr__(self, "validation_rules", tuple(self.validation_rules))
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        object.__setattr__(self, "fallback_source_specification_ids", tuple(self.fallback_source_specification_ids))

    @property
    def required(self) -> bool:
        return bool(self.parameters.get("required", False))

    def to_dict(self) -> dict[str, Any]:
        return to_json_safe(
            {
                "indicator_id": self.indicator_id,
                "display_name": self.display_name,
                "family": self.family,
                "classification": self.classification,
                "purpose": self.purpose,
                "provider": self.provider,
                "source_specification_id": self.source_specification_id,
                "source_operation": self.source_operation,
                "source_fields": self.source_fields,
                "geography_level": self.geography_level,
                "frequency": self.frequency,
                "unit": self.unit,
                "transformation": self.transformation,
                "direction_semantics": self.direction_semantics,
                "required_inputs": self.required_inputs,
                "optional_inputs": self.optional_inputs,
                "freshness_policy": self.freshness_policy,
                "missing_policy": self.missing_policy,
                "validation_rules": self.validation_rules,
                "evidence_template": self.evidence_template,
                "schema_version": self.schema_version,
                "transformation_version": self.transformation_version,
                "active": self.active,
                "owner": self.owner,
                "notes": self.notes,
                "parameters": dict(self.parameters),
                "fallback_source_specification_ids": self.fallback_source_specification_ids,
            },
        )


def initial_indicator_specifications() -> tuple[IndicatorSpecification, ...]:
    monthly = FreshnessPolicy("monthly", 45, 120, 365, "Monthly source cadence; no fabricated publication lag.")
    annual = FreshnessPolicy("annual", 450, 900, 1800, "Annual source cadence; no fabricated publication date.")
    acs = FreshnessPolicy("five_year_estimate", 730, 1460, 2190, "ACS five-year estimates remain five-year estimates.")
    return (
        IndicatorSpecification(
            "unemployment_rate",
            "Unemployment Rate",
            "labor",
            "context",
            "Current labor-market slack measured from governed BLS unemployment source.",
            "bls",
            "bls_national_labor_unemployment",
            "timeseries",
            ("value", "measurement_period", "series_id"),
            "country",
            "monthly",
            "percent",
            "latest_valid",
            "lower_is_improving",
            ("value", "measurement_period", "series_id"),
            (),
            monthly,
            "missing remains unavailable",
            ("numeric_value", "series_id_filter"),
            "{source} measured {display_name} at {value} {unit} for {period}.",
            parameters={"series_id": "LNS14000000", "required": True},
        ),
        IndicatorSpecification("payroll_employment", "Payroll Employment", "labor", "context", "Current employment level; unavailable until governed payroll source exists.", "bls", "bls_national_payroll_employment", "timeseries", ("value", "measurement_period"), "country", "monthly", "thousands", "latest_valid", "higher_is_improving", ("value", "measurement_period"), (), monthly, "missing remains unavailable", ("numeric_value",), "{source} measured {display_name} at {value} {unit} for {period}."),
        IndicatorSpecification("labor_force_participation", "Labor Force Participation", "labor", "context", "Cyclical labor participation context; classified as context because Stage 9 uses current monthly observations.", "bls", "bls_national_labor_force_participation", "timeseries", ("value", "measurement_period"), "country", "monthly", "percent", "latest_valid", "contextual_only", ("value", "measurement_period"), (), monthly, "missing remains unavailable", ("numeric_value",), "{source} measured {display_name} at {value} {unit} for {period}."),
        IndicatorSpecification("median_household_income", "Median Household Income", "income", "structure", "Slow-moving household income structure from ACS.", "census_acs", "census_acs_state_income", "data", ("variables.B19013_001E.value", "state", "NAME"), "state", "five_year_estimate", "usd", "parse_numeric", "contextual_only", ("variables.B19013_001E.value", "state"), ("NAME",), acs, "suppressed or missing ACS values remain unavailable", ("numeric_value", "state_fips"), "{source} measured {display_name} at {value} {unit} for {period}.", parameters={"variable": "B19013_001E"}),
        IndicatorSpecification("per_capita_income", "Per Capita Income", "income", "structure", "Slow-moving per-capita income structure from governed source.", "census_acs", "census_acs_state_income", "data", ("variables.B19301_001E.value", "state", "NAME"), "state", "five_year_estimate", "usd", "parse_numeric", "contextual_only", ("variables.B19301_001E.value", "state"), ("NAME",), acs, "suppressed or missing ACS values remain unavailable", ("numeric_value", "state_fips"), "{source} measured {display_name} at {value} {unit} for {period}.", parameters={"variable": "B19301_001E"}),
        IndicatorSpecification("home_price_index", "Home Price Index", "housing", "context", "Current FHFA home-price level context.", "fhfa", "fhfa_hpi_national", "hpi_fetch", ("index_value", "period"), "country", "monthly", "index", "latest_valid", "contextual_only", ("index_value", "period"), (), monthly, "missing remains unavailable", ("numeric_value", "period"), "{source} measured {display_name} at {value} {unit} for {period}.", parameters={"required": True}),
        IndicatorSpecification("home_price_change", "Home Price Change", "housing", "context", "Deterministic FHFA percent change only where comparable periods are available.", "fhfa", "fhfa_hpi_national", "hpi_fetch", ("index_value", "period"), "country", "monthly", "percent", "percent_change", "contextual_only", ("index_value", "period"), (), monthly, "missing or divide-by-zero remains unavailable", ("numeric_value", "comparable_period"), "{source} derived {display_name} at {value} {unit} for {period}."),
        IndicatorSpecification("fair_market_rent_two_bedroom", "Fair Market Rent Two Bedroom", "housing", "context", "HUD two-bedroom FMR context; unavailable until governed FMR artifact exists.", "hud", "hud_fmr_two_bedroom", "fair_market_rent", ("rent", "bedrooms"), "county", "annual", "usd_per_month", "select_bedroom_count", "contextual_only", ("rent", "bedrooms"), (), annual, "missing remains unavailable", ("numeric_value", "bedroom_count"), "{source} measured {display_name} at {value} {unit} for {period}.", parameters={"bedrooms": 2}),
        IndicatorSpecification("median_home_value", "Median Home Value", "housing", "structure", "Slow-moving owner-occupied home value structure from ACS.", "census_acs", "census_acs_state_home_value", "data", ("variables.B25077_001E.value", "state", "NAME"), "state", "five_year_estimate", "usd", "parse_numeric", "contextual_only", ("variables.B25077_001E.value", "state"), ("NAME",), acs, "suppressed or missing ACS values remain unavailable", ("numeric_value", "state_fips"), "{source} measured {display_name} at {value} {unit} for {period}.", parameters={"variable": "B25077_001E"}),
        IndicatorSpecification("total_population", "Total Population", "population", "structure", "Population base from ACS five-year state observations.", "census_acs", "census_acs_state_population", "data", ("variables.B01001_001E.value", "state", "NAME"), "state", "five_year_estimate", "persons", "parse_numeric", "contextual_only", ("variables.B01001_001E.value", "state"), ("NAME",), acs, "suppressed or missing ACS values remain unavailable", ("numeric_value", "state_fips"), "{source} measured {display_name} at {value} {unit} for {period}.", parameters={"variable": "B01001_001E", "required": True}),
        IndicatorSpecification("real_gdp", "Real GDP", "production_activity", "context", "Current production/activity level from governed BEA/FRED source when available.", "bea", "bea_regional_real_gdp", "regional", ("value", "time_period"), "state", "annual", "millions_2017_dollars", "latest_valid", "contextual_only", ("value", "time_period"), (), annual, "missing remains unavailable", ("numeric_value", "period"), "{source} measured {display_name} at {value} {unit} for {period}."),
        IndicatorSpecification("real_gdp_change", "Real GDP Change", "production_activity", "context", "Deterministic real GDP change only where comparable periods are available.", "bea", "bea_regional_real_gdp", "regional", ("value", "time_period"), "state", "annual", "percent", "percent_change", "contextual_only", ("value", "time_period"), (), annual, "missing or divide-by-zero remains unavailable", ("numeric_value", "comparable_period"), "{source} derived {display_name} at {value} {unit} for {period}."),
    )


def validate_indicator_specifications(specifications: Sequence[IndicatorSpecification]) -> None:
    seen: set[str] = set()
    for spec in specifications:
        if spec.indicator_id in seen:
            raise IndicatorSpecificationError("duplicate indicator_id")
        seen.add(spec.indicator_id)


def _safe_slug(value: str) -> bool:
    return bool(value) and all(ch.isalnum() or ch in {"_", "-"} for ch in value) and ".." not in value

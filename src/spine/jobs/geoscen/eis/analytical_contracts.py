from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Sequence

from spine.jobs.geoscen.eis.contracts import _reject_unsafe, to_json_safe, utc_now_iso

ANALYTICAL_SCHEMA_VERSION = "geoscen.eis.structure_context.v1"
INDICATOR_SPECIFICATIONS_VERSION = "geoscen.eis.indicator_specifications.v1"
SERVING_ARTIFACT_TYPE = "eis_structure_context_serving"

ALLOWED_CLASSIFICATIONS = {"structure", "context"}
ALLOWED_COMPLETION_STATUS = {"complete", "partial", "unavailable", "stale", "invalid"}
ALLOWED_DIRECTIONS = {
    "higher_is_improving",
    "lower_is_improving",
    "neutral_direction",
    "contextual_only",
    "bounded_target_required",
    "unavailable",
}
ALLOWED_FAMILIES = {
    "labor",
    "income",
    "housing",
    "population",
    "production_activity",
    "fiscal_government",
    "external_trade",
}
ALLOWED_FRESHNESS = {"fresh", "current", "stale", "very_stale", "unavailable", "invalid"}
ALLOWED_FREQUENCIES = {"monthly", "quarterly", "annual", "five_year_estimate", "point_in_time", "irregular", "metadata"}
ALLOWED_GEOGRAPHY_LEVELS = {"country", "state", "county", "metro", "unknown"}
ALLOWED_OBSERVATION_STATUS = {"measured", "derived", "unavailable", "stale", "invalid"}


class AnalyticalContractError(ValueError):
    pass


@dataclass(frozen=True)
class FreshnessPolicy:
    expected_frequency: str
    fresh_window_days: int
    stale_window_days: int
    unavailable_after_days: int
    release_lag_notes: str = ""

    def __post_init__(self) -> None:
        _require(self.expected_frequency in ALLOWED_FREQUENCIES, "expected_frequency invalid")
        if min(self.fresh_window_days, self.stale_window_days, self.unavailable_after_days) < 0:
            raise AnalyticalContractError("freshness windows must be non-negative")
        if not self.fresh_window_days <= self.stale_window_days <= self.unavailable_after_days:
            raise AnalyticalContractError("freshness windows must be ordered")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AnalyticalObservation:
    indicator_id: str
    display_name: str
    family: str
    classification: str
    geography: Mapping[str, Any]
    period: Mapping[str, Any]
    value: float | int | None
    raw_value: Any
    unit: str
    status: str
    freshness: str
    direction: str
    source: Mapping[str, Any]
    source_specification_id: str
    provider: str
    transformation: str
    derived: bool
    input_references: Sequence[Mapping[str, Any]]
    evidence_ids: Sequence[str]
    validation: Mapping[str, Any]
    lineage_reference: Mapping[str, Any]
    measurement_as_of: str | None
    publication_date: str | None
    retrieval_timestamp: str | None
    completion_status: str
    warnings: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        _reject_unsafe(asdict(self))
        _require(self.family in ALLOWED_FAMILIES, "family invalid")
        _require(self.classification in ALLOWED_CLASSIFICATIONS, "classification invalid")
        _require(self.status in ALLOWED_OBSERVATION_STATUS, "status invalid")
        _require(self.freshness in ALLOWED_FRESHNESS, "freshness invalid")
        _require(self.completion_status in ALLOWED_COMPLETION_STATUS, "completion_status invalid")
        _require(self.direction in ALLOWED_DIRECTIONS, "direction invalid")
        object.__setattr__(self, "geography", dict(self.geography))
        object.__setattr__(self, "period", dict(self.period))
        object.__setattr__(self, "source", dict(self.source))
        object.__setattr__(self, "input_references", tuple(dict(row) for row in self.input_references))
        object.__setattr__(self, "evidence_ids", tuple(self.evidence_ids))
        object.__setattr__(self, "validation", dict(self.validation))
        object.__setattr__(self, "lineage_reference", dict(self.lineage_reference))
        object.__setattr__(self, "warnings", tuple(self.warnings))

    def to_dict(self) -> dict[str, Any]:
        return to_json_safe(asdict(self))


@dataclass(frozen=True)
class StructureContextServingArtifact:
    run_id: str
    generated_at: str
    domain: str
    geography_scope: Sequence[str]
    source_manifest_reference: Mapping[str, Any] | None
    completion: Mapping[str, Any]
    families: Sequence[Mapping[str, Any]]
    observations: Sequence[Mapping[str, Any]]
    evidence: Sequence[Mapping[str, Any]]
    unavailable: Sequence[Mapping[str, Any]]
    validation: Mapping[str, Any]
    lineage_index: Sequence[Mapping[str, Any]]
    source_summary: Sequence[Mapping[str, Any]]
    warnings: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        _reject_unsafe(asdict(self))
        object.__setattr__(self, "geography_scope", tuple(self.geography_scope))
        object.__setattr__(self, "completion", dict(self.completion))
        object.__setattr__(self, "families", tuple(dict(row) for row in self.families))
        object.__setattr__(self, "observations", tuple(dict(row) for row in self.observations))
        object.__setattr__(self, "evidence", tuple(dict(row) for row in self.evidence))
        object.__setattr__(self, "unavailable", tuple(dict(row) for row in self.unavailable))
        object.__setattr__(self, "validation", dict(self.validation))
        object.__setattr__(self, "lineage_index", tuple(dict(row) for row in self.lineage_index))
        object.__setattr__(self, "source_summary", tuple(dict(row) for row in self.source_summary))
        object.__setattr__(self, "warnings", tuple(self.warnings))

    def to_dict(self) -> dict[str, Any]:
        return to_json_safe(
            {
                "schema_version": ANALYTICAL_SCHEMA_VERSION,
                "artifact_type": SERVING_ARTIFACT_TYPE,
                "generated_at": self.generated_at or utc_now_iso(),
                "run_id": self.run_id,
                "domain": self.domain,
                "geography_scope": self.geography_scope,
                "indicator_specifications_version": INDICATOR_SPECIFICATIONS_VERSION,
                "source_manifest_reference": self.source_manifest_reference,
                "completion": self.completion,
                "families": self.families,
                "observations": self.observations,
                "evidence": self.evidence,
                "unavailable": self.unavailable,
                "validation": self.validation,
                "lineage_index": self.lineage_index,
                "source_summary": self.source_summary,
                "warnings": self.warnings,
            },
        )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AnalyticalContractError(message)

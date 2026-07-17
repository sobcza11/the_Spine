from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence

from spine.jobs.geoscen.eis.contracts import to_json_safe
from spine.jobs.geoscen.eis.indicator_specifications import IndicatorSpecification


def ledger_entry(
    spec: IndicatorSpecification,
    *,
    observations: Sequence[Mapping[str, Any]],
    source_specification_exists: bool,
    source_artifact_available: bool,
    required_fields_available: bool,
    transformation_passed: bool,
    validation_passed: bool,
    evidence_created: bool,
    warnings: Sequence[str],
) -> dict[str, Any]:
    freshness_status = _freshness_from_observations(observations)
    serving_created = bool(observations)
    final_status = _final_status(source_artifact_available, required_fields_available, transformation_passed, validation_passed, evidence_created, serving_created, freshness_status)
    reason = None if final_status == "complete" else ";".join(sorted(set(warnings))) or "indicator_unavailable"
    return to_json_safe(
        {
            "indicator_id": spec.indicator_id,
            "specification_exists": True,
            "source_specification_exists": source_specification_exists,
            "source_artifact_available": source_artifact_available,
            "required_fields_available": required_fields_available,
            "transformation_passed": transformation_passed,
            "validation_passed": validation_passed,
            "evidence_created": evidence_created,
            "serving_observation_created": serving_created,
            "freshness_status": freshness_status,
            "final_status": final_status,
            "unavailable_reason": reason,
        },
    )


def build_completion_ledger(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(entry["final_status"]) for entry in entries)
    return to_json_safe({"entries": tuple(entries), "summary": {"total": len(entries), "complete": counts["complete"], "partial": counts["partial"], "unavailable": counts["unavailable"], "stale": counts["stale"], "invalid": counts["invalid"]}})


def _freshness_from_observations(observations: Sequence[Mapping[str, Any]]) -> str:
    values = {str(row.get("freshness")) for row in observations}
    if not values:
        return "unavailable"
    if "invalid" in values:
        return "invalid"
    if "very_stale" in values:
        return "very_stale"
    if "stale" in values:
        return "stale"
    if values <= {"fresh", "current"}:
        return "current"
    return "unavailable"


def _final_status(source_available: bool, fields_available: bool, transformation_passed: bool, validation_passed: bool, evidence_created: bool, serving_created: bool, freshness_status: str) -> str:
    if not validation_passed:
        return "invalid"
    if freshness_status in {"stale", "very_stale"} and source_available and fields_available:
        return "stale"
    if all((source_available, fields_available, transformation_passed, evidence_created, serving_created)):
        return "complete"
    if source_available and serving_created:
        return "partial"
    return "unavailable"

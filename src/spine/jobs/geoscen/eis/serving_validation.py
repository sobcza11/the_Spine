from __future__ import annotations

from typing import Any, Mapping

from spine.jobs.geoscen.eis.analytical_contracts import ANALYTICAL_SCHEMA_VERSION, SERVING_ARTIFACT_TYPE

PROHIBITED_TERMS = ("api_key", "apikey", "authorization", "bearer", "password", "secret", "token", "score", "ranking", "forecast", "scenario")


class ServingValidationError(ValueError):
    pass


def validate_serving_artifact(payload: Mapping[str, Any]) -> None:
    errors: list[str] = []
    if payload.get("schema_version") != ANALYTICAL_SCHEMA_VERSION:
        errors.append("schema_version_invalid")
    if payload.get("artifact_type") != SERVING_ARTIFACT_TYPE:
        errors.append("artifact_type_invalid")
    observations = _list(payload.get("observations"), "observations", errors)
    evidence = _list(payload.get("evidence"), "evidence", errors)
    families = _list(payload.get("families"), "families", errors)
    completion = payload.get("completion")
    if not isinstance(completion, Mapping):
        errors.append("completion_not_mapping")
    evidence_ids = [row.get("evidence_id") for row in evidence if isinstance(row, Mapping)]
    if len(evidence_ids) != len(set(evidence_ids)):
        errors.append("duplicate_evidence_id")
    known_evidence = set(evidence_ids)
    for index, row in enumerate(observations):
        if not isinstance(row, Mapping):
            errors.append(f"observation:{index}:not_mapping")
            continue
        for key in ("indicator_id", "display_name", "classification", "geography", "period", "status", "freshness", "source", "transformation", "evidence_ids", "lineage_reference"):
            if key not in row:
                errors.append(f"observation:{index}:missing:{key}")
        for evidence_id in row.get("evidence_ids", ()):
            if evidence_id not in known_evidence:
                errors.append(f"observation:{index}:evidence_missing")
    for index, row in enumerate(families):
        if isinstance(row, Mapping) and "score" in repr(row).lower():
            errors.append(f"family:{index}:score_forbidden")
    text = repr(payload).lower()
    for term in PROHIBITED_TERMS:
        if term in text:
            errors.append(f"prohibited_term:{term}")
    if errors:
        raise ServingValidationError(";".join(errors))


def _list(value: Any, name: str, errors: list[str]) -> list[Any]:
    if not isinstance(value, list):
        errors.append(f"{name}_not_list")
        return []
    return value

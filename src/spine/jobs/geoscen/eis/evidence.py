from __future__ import annotations

import hashlib
from typing import Any, Mapping, Sequence

from spine.jobs.geoscen.eis.contracts import canonical_json, to_json_safe

ALLOWED_CLAIM_TYPES = {
    "measured_value",
    "measured_change",
    "source_unavailable",
    "stale_measurement",
    "validation_warning",
    "derived_value",
}


def evidence_id(*, indicator_id: str, source_specification_id: str, geography_id: str, period: str) -> str:
    payload = canonical_json({"indicator_id": indicator_id, "source_specification_id": source_specification_id, "geography_id": geography_id, "period": period})
    return f"eis_ev_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]}"


def build_evidence_record(
    *,
    evidence_id_value: str,
    indicator_id: str,
    geography: Mapping[str, Any],
    period: Mapping[str, Any],
    claim_type: str,
    observation: str,
    source_artifact: Mapping[str, Any] | None,
    source_fields: Sequence[str],
    transformation_version: str,
    lineage_reference: Mapping[str, Any],
    confidence: str,
    limitations: Sequence[str],
) -> dict[str, Any]:
    if claim_type not in ALLOWED_CLAIM_TYPES:
        raise ValueError("claim_type invalid")
    source_metadata = dict(source_artifact.get("source_metadata", {})) if source_artifact else {}
    return to_json_safe(
        {
            "evidence_id": evidence_id_value,
            "indicator_id": indicator_id,
            "geography": geography,
            "period": period,
            "claim_type": claim_type,
            "observation": observation,
            "source": {
                "provider": source_artifact.get("provider") if source_artifact else None,
                "operation": source_artifact.get("operation") if source_artifact else None,
                "source_metadata": source_metadata,
            },
            "source_artifact": {
                "specification_id": source_artifact.get("specification_id") if source_artifact else None,
                "run_id": source_artifact.get("run_id") if source_artifact else None,
                "artifact_type": source_artifact.get("artifact_type") if source_artifact else None,
            },
            "source_fields": tuple(source_fields),
            "source_measurement_as_of": period.get("measurement_as_of"),
            "retrieval_timestamp": period.get("retrieval_timestamp") or (source_artifact.get("retrieved_at") if source_artifact else None),
            "transformation_version": transformation_version,
            "confidence": confidence,
            "limitations": tuple(limitations),
            "lineage_reference": lineage_reference,
        },
    )

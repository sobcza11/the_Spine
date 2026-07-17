from __future__ import annotations

import pytest

from spine.jobs.geoscen.eis.serving_validation import ServingValidationError, validate_serving_artifact


def test_serving_validation_rejects_prohibited_terms() -> None:
    payload = {
        "schema_version": "geoscen.eis.structure_context.v1",
        "artifact_type": "eis_structure_context_serving",
        "observations": [],
        "evidence": [{"evidence_id": "x", "api_key": "bad"}],
    }
    with pytest.raises(ServingValidationError):
        validate_serving_artifact(payload)

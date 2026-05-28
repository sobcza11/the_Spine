from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\causal_inference_validation_layer.json"
)

def test_causal_inference_validation_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "causal-inference-validation-layer"
    assert d["causal_inference_validation_enabled"] is True
    assert d["causal_validation_check_count"] >= 7

    assert "correlation_vs_causality_check" in d["causal_validation_checks"]
    assert "confounder_identification_check" in d["causal_validation_checks"]
    assert "unsupported_causal_claim_rejection" in d["causal_validation_checks"]

    assert d["causal_contract"]["unsupported_causal_claims_forbidden"] is True
    assert d["causal_contract"]["mechanism_evidence_required"] is True

    assert d["governance"]["correlation_not_sufficient"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

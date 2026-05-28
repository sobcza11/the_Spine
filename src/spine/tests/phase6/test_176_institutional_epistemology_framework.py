from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\institutional_epistemology_framework.json"
)

def test_institutional_epistemology_framework():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-epistemology-framework"
    assert d["institutional_epistemology_enabled"] is True
    assert d["epistemic_principle_count"] >= 7

    assert "evidence_before_conviction" in d["epistemic_principles"]
    assert "causality_over_correlation" in d["epistemic_principles"]

    assert d["epistemology_contract"]["contradiction_preservation_required"] is True
    assert d["epistemology_contract"]["causal_validation_required"] is True

    assert d["governance"]["unsupported_conviction_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

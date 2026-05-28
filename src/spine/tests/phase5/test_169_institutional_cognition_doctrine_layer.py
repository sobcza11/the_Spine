from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\institutional_cognition_doctrine_layer.json"
)

def test_institutional_cognition_doctrine_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-cognition-doctrine-layer"
    assert d["institutional_cognition_doctrine_enabled"] is True
    assert d["doctrine_principle_count"] >= 7

    assert "confidence_must_be_calibrated" in d["doctrine_principles"]
    assert "governance_overrides_model_speed" in d["doctrine_principles"]

    assert d["doctrine_contract"]["causal_validation_required"] is True
    assert d["doctrine_contract"]["human_authority_required"] is True

    assert d["governance"]["ungoverned_cognition_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

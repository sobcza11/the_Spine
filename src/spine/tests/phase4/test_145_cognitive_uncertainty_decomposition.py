from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\cognitive_uncertainty_decomposition.json"
)

def test_cognitive_uncertainty_decomposition():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cognitive-uncertainty-decomposition"
    assert d["uncertainty_decomposition_enabled"] is True
    assert d["uncertainty_component_count"] >= 7

    assert "data_uncertainty" in d["uncertainty_components"]
    assert "contradiction_uncertainty" in d["uncertainty_components"]
    assert "operator_uncertainty" in d["uncertainty_components"]

    assert d["uncertainty_contract"]["confidence_change_explanation_required"] is True
    assert d["uncertainty_contract"]["source_uncertainty_required"] is True

    assert d["governance"]["confidence_not_overstated"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

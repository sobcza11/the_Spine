from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\institutional_cognition_constitutional_layer.json"
)

def test_institutional_cognition_constitutional_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-cognition-constitutional-layer"
    assert d["constitutional_layer_enabled"] is True
    assert d["constitutional_principle_count"] >= 7

    assert "human_authority_supremacy" in d["constitutional_principles"]
    assert "contradiction_preservation" in d["constitutional_principles"]

    assert d["constitutional_contract"]["constitutional_alignment_required"] is True
    assert d["governance"]["constitutional_violations_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

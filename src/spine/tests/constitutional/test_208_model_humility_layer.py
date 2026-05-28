from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\model_humility_layer.json"
)

def test_model_humility_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "model-humility-layer"
    assert d["model_humility_enabled"] is True
    assert d["humility_requirement_count"] >= 7

    assert "blind_spot_disclosure" in d["humility_requirements"]
    assert "alternative_explanations" in d["humility_requirements"]

    assert d["humility_contract"]["limitations_required"] is True
    assert d["humility_contract"]["uncertainty_required"] is True

    assert d["governance"]["false_omniscience_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_trust_calibration_layer.json"
)

def test_institutional_trust_calibration_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-trust-calibration-layer"
    assert d["trust_calibration_enabled"] is True
    assert d["trust_domain_count"] > 0

    assert d["trust_contract"]["trust_scores_required"] is True
    assert d["trust_contract"]["confidence_visible"] is True
    assert d["trust_contract"]["human_review_required"] is True

    assert d["governance"]["trust_calibration_governed"] is True
    assert d["governance"]["trust_is_advisory"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

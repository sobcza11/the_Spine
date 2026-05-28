from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_confidence_score_calibration.json"
)

def test_tier7_confidence_score_calibration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-confidence-score-calibration"
    assert d["confidence_score_calibration_enabled"] is True
    assert d["confidence_component_count"] >= 7

    assert d["total_component_weight"] == 1.0

    assert "source_quality" in d["confidence_components"]
    assert "data_freshness" in d["confidence_components"]
    assert "contradiction_penalty" in d["confidence_components"]

    assert d["confidence_contract"]["confidence_is_measured"] is True
    assert d["confidence_contract"]["confidence_is_not_certainty"] is True

    assert d["governance"]["confidence_calibration_governed"] is True
    assert d["governance"]["confidence_is_advisory"] is True

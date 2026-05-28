from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\constitutional_readiness_scorecard.json"
)

def test_constitutional_readiness_scorecard():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "constitutional-readiness-scorecard"
    assert d["constitutional_readiness_scorecard_enabled"] is True
    assert d["readiness_area_count"] >= 7

    assert d["overall_constitutional_readiness_score"] >= 9.0

    assert "truth_governance" in d["readiness_areas"]
    assert "trust_calibration" in d["readiness_areas"]

    assert d["scorecard_contract"]["quantitative_scoring_required"] is True
    assert d["governance"]["constitutional_drift_tracking_required"] is True

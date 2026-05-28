from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\constitutional_violation_detector.json"
)

def test_constitutional_violation_detector():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "constitutional-violation-detector"
    assert d["constitutional_violation_detector_enabled"] is True
    assert d["violation_class_count"] >= 7

    assert "evidence_violation" in d["violation_classes"]
    assert "human_authority_violation" in d["violation_classes"]

    assert d["detector_contract"]["severity_scoring_required"] is True
    assert d["detector_contract"]["escalation_required"] is True

    assert d["governance"]["constitutional_enforcement_required"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

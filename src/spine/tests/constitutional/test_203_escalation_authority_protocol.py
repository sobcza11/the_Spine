from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\escalation_authority_protocol.json"
)

def test_escalation_authority_protocol():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "escalation-authority-protocol"
    assert d["escalation_authority_protocol_enabled"] is True
    assert d["escalation_class_count"] >= 7

    assert "constitutional_violation_escalation" in d["escalation_classes"]
    assert "operator_review_escalation" in d["escalation_classes"]

    assert d["escalation_contract"]["escalation_reason_required"] is True
    assert d["escalation_contract"]["authority_boundary_required"] is True

    assert d["governance"]["unreviewed_escalation_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

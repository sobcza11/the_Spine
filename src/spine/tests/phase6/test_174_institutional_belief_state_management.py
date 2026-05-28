from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\institutional_belief_state_management.json"
)

def test_institutional_belief_state_management():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-belief-state-management"
    assert d["belief_state_management_enabled"] is True
    assert d["belief_state_field_count"] >= 8

    assert "belief_statement" in d["belief_state_fields"]
    assert "contradicting_evidence" in d["belief_state_fields"]
    assert "change_history" in d["belief_state_fields"]

    assert d["belief_contract"]["confidence_required"] is True
    assert d["belief_contract"]["contradicting_evidence_required"] is True

    assert d["governance"]["untracked_belief_drift_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

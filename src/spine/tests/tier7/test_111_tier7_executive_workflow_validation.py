from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_executive_workflow_validation.json"
)

def test_tier7_executive_workflow_validation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-executive-workflow-validation"
    assert d["executive_workflow_validation_enabled"] is True
    assert d["executive_workflow_count"] >= 7

    assert "analyst_review_loop" in d["executive_workflows"]
    assert "executive_escalation_loop" in d["executive_workflows"]
    assert "post_decision_audit_loop" in d["executive_workflows"]

    assert d["workflow_contract"]["analyst_review_required"] is True
    assert d["workflow_contract"]["governance_approval_required"] is True
    assert d["workflow_contract"]["post_decision_audit_required"] is True

    assert d["governance"]["workflow_validation_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_workflow_engine.json"
)

def test_institutional_workflow_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-workflow-engine"
    assert d["workflow_engine_enabled"] is True
    assert d["workflow_lane_count"] > 0

    assert d["workflow_contract"]["human_review_required"] is True
    assert d["workflow_contract"]["escalation_paths_required"] is True
    assert d["workflow_contract"]["decision_trace_required"] is True

    assert d["governance"]["workflow_governance_enabled"] is True
    assert d["governance"]["autonomous_execution_allowed"] is False
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_institutional_frontend_unification.json"
)

def test_tier7_institutional_frontend_unification():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-institutional-frontend-unification"
    assert d["frontend_unification_enabled"] is True
    assert d["frontend_panel_count"] >= 8

    assert "executive_overview_panel" in d["frontend_panels"]
    assert "risk_command_panel" in d["frontend_panels"]
    assert "governance_review_panel" in d["frontend_panels"]

    assert d["frontend_contract"]["single_executive_environment_required"] is True
    assert d["frontend_contract"]["json_artifacts_remain_source_of_truth"] is True
    assert d["frontend_contract"]["react_frontend_future_ready"] is True

    assert d["ui_policy"]["executive_first"] is True
    assert d["ui_policy"]["decision_support_only"] is True

    assert d["governance"]["frontend_unification_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

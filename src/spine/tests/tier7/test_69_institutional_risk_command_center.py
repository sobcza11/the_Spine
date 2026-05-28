from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\institutional_risk_command_center.json"
)

def test_institutional_risk_command_center():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-risk-command-center"
    assert d["risk_command_center_enabled"] is True
    assert d["risk_channel_count"] > 0

    assert d["risk_contract"]["risk_state_visible"] is True
    assert d["risk_contract"]["executive_escalation_supported"] is True
    assert d["risk_contract"]["decision_support_only"] is True

    assert d["governance"]["risk_command_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["autonomous_execution_allowed"] is False

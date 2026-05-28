from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\strategic_alerting_engine.json"
)

def test_strategic_alerting_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "strategic-alerting-engine"
    assert d["strategic_alerting_enabled"] is True
    assert d["alert_channel_count"] >= 7

    assert "liquidity_stress_alert" in d["alert_channels"]
    assert "sovereign_deterioration_alert" in d["alert_channels"]
    assert "operator_escalation_alert" in d["alert_channels"]

    assert d["alerting_contract"]["operator_review_required"] is True
    assert d["alerting_contract"]["false_positive_tracking_required"] is True

    assert d["governance"]["decision_execution_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

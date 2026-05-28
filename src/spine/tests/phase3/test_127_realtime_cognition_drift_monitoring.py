from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\realtime_cognition_drift_monitoring.json"
)

def test_realtime_cognition_drift_monitoring():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "realtime-cognition-drift-monitoring"
    assert d["cognition_drift_monitoring_enabled"] is True
    assert d["drift_monitoring_channel_count"] >= 7

    assert "confidence_score_drift" in d["drift_monitoring_channels"]
    assert "agent_reasoning_drift" in d["drift_monitoring_channels"]
    assert "operator_override_drift" in d["drift_monitoring_channels"]

    assert d["drift_contract"]["runtime_degradation_detection_required"] is True
    assert d["governance"]["runtime_degradation_visible"] is True

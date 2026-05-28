from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\cognitive_burden_monitor.json"
)

def test_cognitive_burden_monitor():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cognitive-burden-monitor"
    assert d["cognitive_burden_monitor_enabled"] is True
    assert d["burden_signal_count"] >= 7

    assert "excessive_alert_volume" in d["burden_signals"]
    assert "decision_latency_increase" in d["burden_signals"]

    assert d["burden_contract"]["signal_to_noise_tracking_required"] is True
    assert d["burden_contract"]["priority_ranking_required"] is True

    assert d["governance"]["operator_overload_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

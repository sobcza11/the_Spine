from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\false_positive_reduction_engine.json"
)

def test_false_positive_reduction_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "false-positive-reduction-engine"
    assert d["false_positive_reduction_enabled"] is True
    assert d["reduction_control_count"] >= 7

    assert "multi_signal_confirmation_required" in d["reduction_controls"]
    assert "operator_review_before_escalation" in d["reduction_controls"]
    assert "historical_false_positive_memory_check" in d["reduction_controls"]

    assert d["reduction_contract"]["false_positive_memory_required"] is True
    assert d["reduction_contract"]["alert_noise_reduction_required"] is True

    assert d["governance"]["suppression_requires_audit"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

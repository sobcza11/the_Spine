from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\trust_reset_protocol.json"
)

def test_trust_reset_protocol():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "trust-reset-protocol"
    assert d["trust_reset_protocol_enabled"] is True
    assert d["trust_reset_trigger_count"] >= 7

    assert "major_forecast_failure" in d["trust_reset_triggers"]
    assert "constitutional_violation" in d["trust_reset_triggers"]

    assert d["reset_contract"]["confidence_recalibration_required"] is True
    assert d["reset_contract"]["operator_review_required"] is True

    assert d["governance"]["automatic_trust_restoration_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

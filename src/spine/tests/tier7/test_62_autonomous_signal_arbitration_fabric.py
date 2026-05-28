from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\autonomous_signal_arbitration_fabric.json"
)

def test_autonomous_signal_arbitration_fabric():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "autonomous-signal-arbitration-fabric"
    assert d["signal_arbitration_enabled"] is True
    assert d["signal_family_count"] > 0

    assert d["arbitration_contract"]["conflicting_signals_preserved"] is True
    assert d["arbitration_contract"]["deterministic_signals_authoritative"] is True
    assert d["arbitration_contract"]["autonomous_execution_blocked"] is True

    assert d["governance"]["signal_arbitration_governed"] is True
    assert d["governance"]["autonomous_execution_allowed"] is False
    assert d["governance"]["llm_writeback_allowed"] is False

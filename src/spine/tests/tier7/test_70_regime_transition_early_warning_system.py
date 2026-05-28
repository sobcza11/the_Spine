from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\regime_transition_early_warning_system.json"
)

def test_regime_transition_early_warning_system():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "regime-transition-early-warning-system"
    assert d["regime_warning_enabled"] is True
    assert d["regime_signal_count"] > 0

    assert d["warning_contract"]["early_warning_supported"] is True
    assert d["warning_contract"]["historical_analog_required"] is True
    assert d["warning_contract"]["human_review_required"] is True

    assert d["governance"]["regime_warning_governed"] is True
    assert d["governance"]["deterministic_inputs_authoritative"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

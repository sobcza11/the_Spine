from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\continuous_historical_replay_engine.json"
)

def test_continuous_historical_replay_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "continuous-historical-replay-engine"
    assert d["historical_replay_enabled"] is True
    assert d["replay_domain_count"] > 0

    assert d["replay_contract"]["historical_continuity_required"] is True
    assert d["replay_contract"]["analog_context_required"] is True
    assert d["replay_contract"]["regime_comparison_supported"] is True

    assert d["governance"]["historical_replay_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["uncited_synthesis_allowed"] is False

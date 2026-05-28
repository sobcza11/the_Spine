from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\institutional_stress_replay_engine.json"
)

def test_institutional_stress_replay_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-stress-replay-engine"
    assert d["stress_replay_engine_enabled"] is True
    assert d["stress_replay_event_count"] >= 7

    assert "global_financial_crisis_2008" in d["stress_replay_events"]
    assert "covid_liquidity_shock_2020" in d["stress_replay_events"]
    assert "inflation_policy_shock_2022" in d["stress_replay_events"]

    assert d["stress_replay_contract"]["failure_mode_attribution_required"] is True
    assert d["governance"]["hindsight_bias_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

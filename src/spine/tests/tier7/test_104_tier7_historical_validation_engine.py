from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_historical_validation_engine.json"
)

def test_tier7_historical_validation_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-historical-validation-engine"
    assert d["historical_validation_enabled"] is True
    assert d["validation_period_count"] >= 5
    assert d["validation_target_count"] >= 5

    assert "gfc_2008" in d["validation_periods"]
    assert "covid_2020" in d["validation_periods"]
    assert "inflation_2022" in d["validation_periods"]

    assert d["historical_validation_contract"]["crisis_replay_required"] is True
    assert d["historical_validation_contract"]["regime_transition_validation_required"] is True

    assert d["governance"]["historical_validation_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

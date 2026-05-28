from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\multi_horizon_forecasting_engine.json"
)

def test_multi_horizon_forecasting_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "multi-horizon-forecasting-engine"
    assert d["forecasting_engine_enabled"] is True
    assert d["forecast_horizon_count"] > 0

    assert d["forecasting_contract"]["forecast_is_advisory"] is True
    assert d["forecasting_contract"]["scenario_uncertainty_required"] is True
    assert d["forecasting_contract"]["human_review_required"] is True

    assert d["governance"]["forecasting_governance_enabled"] is True
    assert d["governance"]["deterministic_inputs_authoritative"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

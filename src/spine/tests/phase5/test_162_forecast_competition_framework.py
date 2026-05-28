from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\forecast_competition_framework.json"
)

def test_forecast_competition_framework():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "forecast-competition-framework"
    assert d["forecast_competition_framework_enabled"] is True
    assert d["forecast_competitor_count"] >= 7

    assert "baseline_naive_forecast" in d["forecast_competitors"]
    assert "human_analyst_forecast" in d["forecast_competitors"]
    assert "multi_signal_composite_forecast" in d["forecast_competitors"]

    assert d["competition_contract"]["baseline_forecast_required"] is True
    assert d["competition_contract"]["forecast_scoring_required"] is True

    assert d["governance"]["winner_promotion_requires_review"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase4\long_horizon_forecast_tracking.json"
)

def test_long_horizon_forecast_tracking():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "long-horizon-forecast-tracking"
    assert d["long_horizon_forecast_tracking_enabled"] is True
    assert d["forecast_horizon_count"] == 3

    assert "3_month" in d["forecast_horizons"]
    assert "6_month" in d["forecast_horizons"]
    assert "12_month" in d["forecast_horizons"]

    assert d["tracking_contract"]["realized_outcome_required"] is True
    assert d["tracking_contract"]["accuracy_scoring_required"] is True

    assert d["governance"]["forecast_tracking_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\executive_research_briefing_theater.json"
)

def test_executive_research_briefing_theater():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "executive-research-briefing-theater"
    assert d["executive_research_briefing_theater_enabled"] is True
    assert d["theater_component_count"] >= 7

    assert "forecast_competition_panels" in d["theater_components"]
    assert "contradiction_heatmaps" in d["theater_components"]

    assert d["theater_contract"]["live_research_visibility_required"] is True
    assert d["theater_contract"]["governance_visibility_required"] is True

    assert d["governance"]["operator_override_visible"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

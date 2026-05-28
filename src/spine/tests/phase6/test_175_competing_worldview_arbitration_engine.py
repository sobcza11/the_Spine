from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\competing_worldview_arbitration_engine.json"
)

def test_competing_worldview_arbitration_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "competing-worldview-arbitration-engine"
    assert d["worldview_arbitration_enabled"] is True
    assert d["worldview_type_count"] >= 7

    assert "inflation_persistence_worldview" in d["worldview_types"]
    assert "sovereign_fragility_worldview" in d["worldview_types"]

    assert d["arbitration_contract"]["competing_worldviews_preserved"] is True
    assert d["arbitration_contract"]["premature_resolution_forbidden"] is True

    assert d["governance"]["single_narrative_capture_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

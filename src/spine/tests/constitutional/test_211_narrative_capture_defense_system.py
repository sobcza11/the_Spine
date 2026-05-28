from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\narrative_capture_defense_system.json"
)

def test_narrative_capture_defense_system():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "narrative-capture-defense-system"
    assert d["narrative_capture_defense_enabled"] is True
    assert d["capture_vector_count"] >= 7

    assert "media_narrative_pressure" in d["capture_vectors"]
    assert "institutional_groupthink" in d["capture_vectors"]

    assert d["defense_contract"]["independent_reasoning_required"] is True
    assert d["defense_contract"]["contradiction_preservation_required"] is True

    assert d["governance"]["groupthink_resistance_required"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\leadership_transition_continuity_engine.json"
)

def test_leadership_transition_continuity_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "leadership-transition-continuity-engine"
    assert d["leadership_transition_continuity_enabled"] is True
    assert d["continuity_component_count"] >= 7

    assert "macro_memory_transfer" in d["continuity_components"]
    assert "constitutional_boundary_retention" in d["continuity_components"]

    assert d["continuity_contract"]["institutional_memory_transfer_required"] is True
    assert d["continuity_contract"]["doctrine_preservation_required"] is True

    assert d["governance"]["institutional_amnesia_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

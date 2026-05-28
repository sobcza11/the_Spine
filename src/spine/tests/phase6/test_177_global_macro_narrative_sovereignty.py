from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\global_macro_narrative_sovereignty.json"
)

def test_global_macro_narrative_sovereignty():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "global-macro-narrative-sovereignty"
    assert d["global_macro_narrative_sovereignty_enabled"] is True
    assert d["narrative_defense_system_count"] >= 7

    assert "propaganda_detection" in d["narrative_defense_systems"]
    assert "consensus_bias_detection" in d["narrative_defense_systems"]

    assert d["narrative_contract"]["cross_source_validation_required"] is True
    assert d["narrative_contract"]["contradictory_sources_preserved"] is True

    assert d["governance"]["narrative_capture_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

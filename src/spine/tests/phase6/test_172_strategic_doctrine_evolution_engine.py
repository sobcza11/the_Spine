from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\strategic_doctrine_evolution_engine.json"
)

def test_strategic_doctrine_evolution_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "strategic-doctrine-evolution-engine"
    assert d["doctrine_evolution_enabled"] is True
    assert d["doctrine_evolution_control_count"] >= 7

    assert "doctrine_change_proposal" in d["doctrine_evolution_controls"]
    assert "doctrine_versioning" in d["doctrine_evolution_controls"]

    assert d["doctrine_contract"]["doctrine_versioning_required"] is True
    assert d["doctrine_contract"]["evidence_required_for_change"] is True

    assert d["governance"]["unreviewed_doctrine_change_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

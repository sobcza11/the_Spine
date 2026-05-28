from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\autonomous_doctrine_consistency_engine.json"
)

def test_autonomous_doctrine_consistency_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "autonomous-doctrine-consistency-engine"
    assert d["doctrine_consistency_engine_enabled"] is True
    assert d["consistency_check_count"] >= 7

    assert "belief_doctrine_alignment" in d["consistency_checks"]
    assert "human_authority_alignment" in d["consistency_checks"]

    assert d["consistency_contract"]["internal_contradiction_detection_required"] is True
    assert d["governance"]["autonomous_doctrine_mutation_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

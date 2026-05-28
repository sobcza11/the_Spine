from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\contradiction_preservation_doctrine.json"
)

def test_contradiction_preservation_doctrine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "contradiction-preservation-doctrine"
    assert d["contradiction_preservation_enabled"] is True
    assert d["contradiction_class_count"] >= 7

    assert "belief_contradiction" in d["contradiction_classes"]
    assert "operator_contradiction" in d["contradiction_classes"]

    assert d["contradiction_contract"]["contradictions_must_remain_visible"] is True
    assert d["contradiction_contract"]["premature_resolution_forbidden"] is True

    assert d["governance"]["false_coherence_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

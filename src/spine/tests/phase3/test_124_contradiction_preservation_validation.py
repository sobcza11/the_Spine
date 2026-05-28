from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\contradiction_preservation_validation.json"
)

def test_contradiction_preservation_validation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "contradiction-preservation-validation"
    assert d["contradiction_preservation_validation_enabled"] is True
    assert d["preservation_check_count"] >= 5

    assert d["preservation_contract"]["contradictions_must_survive"] is True
    assert d["preservation_contract"]["narrative_smoothing_forbidden"] is True
    assert d["governance"]["false_coherence_blocked"] is True

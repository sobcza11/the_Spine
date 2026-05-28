from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\release_candidate_declaration.json"
)

def test_release_candidate_declaration():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "release-candidate-declaration"
    assert d["release_candidate_declaration_enabled"] is True
    assert d["release_candidate_version"] == "RC1"
    assert d["declaration_area_count"] >= 7

    assert "production_validation_incomplete" in d["declaration_areas"]

    assert d["declaration_contract"]["production_gap_visibility_required"] is True
    assert d["governance"]["system_not_declared_finished"] is True

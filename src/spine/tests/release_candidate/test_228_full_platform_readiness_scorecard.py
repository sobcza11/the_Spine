from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\full_platform_readiness_scorecard.json"
)

def test_full_platform_readiness_scorecard():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "full-platform-readiness-scorecard"
    assert d["platform_readiness_scorecard_enabled"] is True
    assert d["readiness_area_count"] >= 8
    assert d["composite_readiness_score"] >= 9.0

    assert d["release_candidate_status"] == "RC1_SCAFFOLD_READY_VALIDATION_REQUIRED"

    assert d["scorecard_contract"]["validation_gap_visible"] is True
    assert d["governance"]["release_candidate_not_marked_done"] is True

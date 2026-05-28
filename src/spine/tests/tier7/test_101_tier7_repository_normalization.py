from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_repository_normalization.json"
)

def test_tier7_repository_normalization():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-repository-normalization"
    assert d["repository_normalization_enabled"] is True
    assert d["normalization_track_count"] >= 5

    assert d["normalization_contract"]["duplicate_detection_required"] is True
    assert d["normalization_contract"]["dead_stub_detection_required"] is True
    assert d["normalization_contract"]["tier_boundary_validation_required"] is True

    assert d["cleanup_policy"]["delete_without_review"] is False
    assert d["cleanup_policy"]["quarantine_before_removal"] is True

    assert d["governance"]["repository_normalization_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

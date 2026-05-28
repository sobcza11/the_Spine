from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\repo_pruning_audit.json"
)

def test_repo_pruning_audit():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "repo-pruning-audit"
    assert d["repo_pruning_audit_enabled"] is True
    assert d["pruning_category_count"] >= 7

    assert "duplicate_modules" in d["pruning_categories"]
    assert "deprecated_scaffolds" in d["pruning_categories"]

    assert d["audit_contract"]["cleanup_visibility_required"] is True
    assert d["governance"]["manual_review_required_before_removal"] is True

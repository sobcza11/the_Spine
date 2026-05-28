from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\external_reviewer_package.json"
)

def test_external_reviewer_package():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "external-reviewer-package"
    assert d["external_reviewer_package_enabled"] is True
    assert d["reviewer_artifact_count"] >= 7

    assert "validation_runner" in d["reviewer_artifacts"]
    assert "real_vs_scaffold_classifier" in d["reviewer_artifacts"]

    assert d["package_contract"]["maturity_boundary_visibility_required"] is True
    assert d["governance"]["validation_gap_visibility_required"] is True

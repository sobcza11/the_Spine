from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_golden_path_test.json"
)

def test_tier7_golden_path_test():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-golden-path-test"
    assert d["golden_path_test_enabled"] is True
    assert d["golden_path_stage_count"] == 7

    assert d["tier7_required_artifact_count"] == d["tier7_existing_artifact_count"]
    assert d["golden_path_passed"] is True

    assert d["golden_path_contract"]["tier1_to_tier7_path_declared"] is True
    assert d["golden_path_contract"]["tier7_terminal_artifacts_present"] is True

    assert d["governance"]["golden_path_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

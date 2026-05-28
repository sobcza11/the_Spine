from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_integration_test.json"
)

def test_tier7_integration_test():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-integration-test"
    assert d["integration_test_enabled"] is True

    assert d["required_artifact_count"] == 33
    assert d["existing_artifact_count"] == 33
    assert d["missing_artifact_count"] == 0
    assert d["integration_passed"] is True

    assert d["integration_contract"]["all_required_modules_present"] is True
    assert d["integration_contract"]["os_scaffold_validated"] is True

    assert d["governance"]["integration_validation_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

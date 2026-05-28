from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\offline_validation_runtime.json"
)

def test_offline_validation_runtime():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-validation-runtime"
    assert d["offline_validation_runtime_enabled"] is True
    assert d["validation_rule_count"] >= 6

    assert "schema_validation" in d["validation_rules"]

    assert d["validation_contract"]["quarantine_on_failure_required"] is True
    assert d["governance"]["invalid_dataset_execution_forbidden"] is True

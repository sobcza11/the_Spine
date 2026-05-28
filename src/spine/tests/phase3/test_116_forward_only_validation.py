from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\forward_only_validation.json"
)

def test_forward_only_validation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "forward-only-validation"
    assert d["forward_only_validation_enabled"] is True
    assert d["validation_rule_count"] >= 5

    assert d["forward_validation_contract"]["future_data_forbidden"] is True
    assert d["forward_validation_contract"]["formula_freeze_required"] is True
    assert d["governance"]["hindsight_bias_blocked"] is True

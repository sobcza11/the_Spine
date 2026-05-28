from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\one_command_validation_runner.json"
)

def test_one_command_validation_runner():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "one-command-validation-runner"
    assert d["one_command_validation_runner_enabled"] is True
    assert d["validation_command_count"] >= 7

    assert d["recommended_command"] == "python -m pytest src/spine/tests -v"

    assert d["runner_contract"]["single_command_validation_required"] is True
    assert d["runner_contract"]["constitutional_tests_included"] is True

    assert d["governance"]["manual_selection_risk_reduced"] is True

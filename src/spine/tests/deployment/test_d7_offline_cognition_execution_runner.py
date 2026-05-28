from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\offline_cognition_execution_runner.json"
)

def test_offline_cognition_execution_runner():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-cognition-execution-runner"
    assert d["offline_cognition_execution_runner_enabled"] is True
    assert d["execution_stage_count"] >= 6

    assert "offline_cognition_execution" in d["execution_stages"]

    assert d["runner_contract"]["deterministic_execution_required"] is True
    assert d["governance"]["ungoverned_execution_forbidden"] is True

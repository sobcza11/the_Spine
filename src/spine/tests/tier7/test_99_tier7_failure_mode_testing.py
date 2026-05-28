from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_failure_mode_testing.json"
)

def test_tier7_failure_mode_testing():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-failure-mode-testing"
    assert d["failure_mode_testing_enabled"] is True
    assert d["failure_mode_count"] >= 5

    assert d["failure_contract"]["missing_file_behavior_defined"] is True
    assert d["failure_contract"]["stale_artifact_behavior_defined"] is True
    assert d["failure_contract"]["cache_rebuild_behavior_defined"] is True

    assert d["fallback_policy"]["missing_file"] == "fail_closed_and_report_missing_artifact"

    assert d["governance"]["failure_testing_governed"] is True
    assert d["governance"]["fail_closed_default"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

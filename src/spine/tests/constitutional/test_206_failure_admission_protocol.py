from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\failure_admission_protocol.json"
)

def test_failure_admission_protocol():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "failure-admission-protocol"
    assert d["failure_admission_protocol_enabled"] is True
    assert d["failure_class_count"] >= 7

    assert "forecast_failure" in d["failure_classes"]
    assert "operator_trust_failure" in d["failure_classes"]

    assert d["protocol_contract"]["failure_record_required"] is True
    assert d["protocol_contract"]["post_mortem_required"] is True

    assert d["governance"]["failure_suppression_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

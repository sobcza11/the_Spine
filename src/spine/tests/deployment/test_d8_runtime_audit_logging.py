from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\runtime_audit_logging.json"
)

def test_runtime_audit_logging():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "runtime-audit-logging"
    assert d["runtime_audit_logging_enabled"] is True
    assert d["audit_event_count"] >= 8

    assert "validation_failed" in d["audit_events"]

    assert d["audit_contract"]["execution_logging_required"] is True
    assert d["governance"]["silent_runtime_operations_forbidden"] is True

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\quarantine_error_handling.json"
)

def test_quarantine_error_handling():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "quarantine-error-handling"
    assert d["quarantine_error_handling_enabled"] is True
    assert d["quarantine_reason_count"] >= 6

    assert "schema_failure" in d["quarantine_reasons"]

    assert d["quarantine_contract"]["runtime_block_required"] is True
    assert d["governance"]["corrupted_data_promotion_forbidden"] is True

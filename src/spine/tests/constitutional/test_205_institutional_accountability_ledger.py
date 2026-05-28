from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\institutional_accountability_ledger.json"
)

def test_institutional_accountability_ledger():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-accountability-ledger"
    assert d["institutional_accountability_ledger_enabled"] is True
    assert d["accountability_field_count"] >= 9
    assert len(d["accountability_schema_hash"]) == 64

    assert "operator_id" in d["accountability_fields"]
    assert "authority_boundary" in d["accountability_fields"]

    assert d["ledger_contract"]["operator_accountability_required"] is True
    assert d["ledger_contract"]["authority_boundary_required"] is True

    assert d["governance"]["anonymous_decision_use_forbidden"] is True
    assert d["governance"]["human_responsibility_visible"] is True

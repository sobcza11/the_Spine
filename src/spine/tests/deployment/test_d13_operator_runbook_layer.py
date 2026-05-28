from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\operator_runbook_layer.json"
)

def test_operator_runbook_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "operator-runbook-layer"
    assert d["operator_runbook_layer_enabled"] is True
    assert d["runbook_procedure_count"] >= 6

    assert "quarantine_review_protocol" in d["runbook_procedures"]

    assert d["runbook_contract"]["deployment_review_required"] is True
    assert d["governance"]["human_authority_required"] is True

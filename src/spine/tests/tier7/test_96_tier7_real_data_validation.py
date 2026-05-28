from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_real_data_validation.json"
)

def test_tier7_real_data_validation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-real-data-validation"
    assert d["real_data_validation_enabled"] is True

    assert d["required_evidence_count"] == 5
    assert d["existing_evidence_count"] == 5
    assert d["measured_evidence_complete"] is True

    assert d["validation_contract"]["claims_backed_by_artifacts"] is True
    assert d["validation_contract"]["scaffold_claims_measured"] is True

    assert d["governance"]["validation_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

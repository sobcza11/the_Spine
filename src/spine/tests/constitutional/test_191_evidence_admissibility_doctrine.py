from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\evidence_admissibility_doctrine.json"
)

def test_evidence_admissibility_doctrine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "evidence-admissibility-doctrine"
    assert d["evidence_admissibility_enabled"] is True
    assert d["admissible_evidence_class_count"] >= 7

    assert "audited_macro_data" in d["admissible_evidence_classes"]
    assert "governed_runtime_artifacts" in d["admissible_evidence_classes"]

    assert d["admissibility_contract"]["unsupported_claims_forbidden"] is True
    assert d["admissibility_contract"]["narrative_only_evidence_insufficient"] is True

    assert d["governance"]["inadmissible_evidence_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\meta_governance_oversight_architecture.json"
)

def test_meta_governance_oversight_architecture():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "meta-governance-oversight-architecture"
    assert d["meta_governance_enabled"] is True
    assert d["meta_governance_domain_count"] >= 7

    assert "governance_rule_review" in d["meta_governance_domains"]
    assert "governance_failure_review" in d["meta_governance_domains"]

    assert d["meta_governance_contract"]["governance_review_required"] is True
    assert d["governance"]["ungoverned_governance_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

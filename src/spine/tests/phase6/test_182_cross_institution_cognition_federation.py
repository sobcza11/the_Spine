from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\cross_institution_cognition_federation.json"
)

def test_cross_institution_cognition_federation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cross-institution-cognition-federation"
    assert d["cross_institution_federation_enabled"] is True
    assert d["federation_domain_count"] >= 7

    assert "permissioned_cognition_exchange" in d["federation_domains"]
    assert "federated_audit_trails" in d["federation_domains"]

    assert d["federation_contract"]["permissioned_exchange_required"] is True
    assert d["governance"]["unpermissioned_exchange_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

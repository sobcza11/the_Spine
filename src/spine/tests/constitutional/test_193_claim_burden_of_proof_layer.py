from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\claim_burden_of_proof_layer.json"
)

def test_claim_burden_of_proof_layer():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "claim-burden-of-proof-layer"
    assert d["claim_burden_of_proof_enabled"] is True
    assert d["claim_requirement_count"] >= 8

    assert "source_provenance" in d["claim_requirements"]
    assert "contradicting_evidence" in d["claim_requirements"]

    assert d["proof_contract"]["evidence_required_for_every_claim"] is True
    assert d["proof_contract"]["unsupported_claims_forbidden"] is True

    assert d["governance"]["claim_without_evidence_blocked"] is True
    assert d["governance"]["hallucinated_conviction_blocked"] is True

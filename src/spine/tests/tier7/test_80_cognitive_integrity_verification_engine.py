from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\cognitive_integrity_verification_engine.json"
)

def test_cognitive_integrity_verification_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cognitive-integrity-verification-engine"
    assert d["integrity_verification_enabled"] is True
    assert d["integrity_domain_count"] > 0

    assert d["integrity_contract"]["provenance_verification_required"] is True
    assert d["integrity_contract"]["deterministic_alignment_required"] is True
    assert d["integrity_contract"]["contradictions_must_survive"] is True

    assert d["governance"]["integrity_verification_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["uncited_synthesis_allowed"] is False

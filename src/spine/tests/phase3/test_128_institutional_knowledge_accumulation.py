from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\institutional_knowledge_accumulation.json"
)

def test_institutional_knowledge_accumulation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-knowledge-accumulation"
    assert d["knowledge_accumulation_enabled"] is True
    assert d["knowledge_domain_count"] >= 8

    assert "validated_signal_history" in d["knowledge_domains"]
    assert "failed_signal_history" in d["knowledge_domains"]
    assert "post_mortem_lessons" in d["knowledge_domains"]

    assert d["knowledge_contract"]["failed_signal_memory_required"] is True
    assert d["knowledge_contract"]["cross_cycle_memory_required"] is True

    assert d["governance"]["memory_write_requires_review"] is True

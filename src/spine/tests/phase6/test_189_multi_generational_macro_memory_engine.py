from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\multi_generational_macro_memory_engine.json"
)

def test_multi_generational_macro_memory_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "multi-generational-macro-memory-engine"
    assert d["multi_generational_memory_enabled"] is True
    assert d["memory_domain_count"] >= 7

    assert "leadership_transition_memory" in d["memory_domains"]
    assert "historical_failure_memory" in d["memory_domains"]

    assert d["memory_contract"]["institutional_lineage_required"] is True
    assert d["governance"]["memory_loss_visibility_required"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\cross_cycle_regime_memory.json"
)

def test_cross_cycle_regime_memory():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cross-cycle-regime-memory"
    assert d["cross_cycle_regime_memory_enabled"] is True
    assert d["regime_memory_domain_count"] >= 8

    assert "inflation_cycle_memory" in d["regime_memory_domains"]
    assert "policy_cycle_memory" in d["regime_memory_domains"]
    assert "cross_asset_fracture_memory" in d["regime_memory_domains"]

    assert d["memory_contract"]["multi_cycle_memory_required"] is True
    assert d["memory_contract"]["failure_memory_required"] is True

    assert d["governance"]["memory_write_requires_review"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

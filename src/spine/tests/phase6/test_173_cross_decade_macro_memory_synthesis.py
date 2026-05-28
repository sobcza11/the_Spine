from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\cross_decade_macro_memory_synthesis.json"
)

def test_cross_decade_macro_memory_synthesis():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cross-decade-macro-memory-synthesis"
    assert d["cross_decade_memory_synthesis_enabled"] is True
    assert d["memory_synthesis_cycle_count"] >= 7

    assert "2008_gfc_memory" in d["memory_synthesis_cycles"]
    assert "2020s_inflation_liquidity_memory" in d["memory_synthesis_cycles"]

    assert d["memory_contract"]["multi_decade_memory_required"] is True
    assert d["memory_contract"]["historical_traceability_required"] is True

    assert d["governance"]["unsupported_analog_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

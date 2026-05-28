from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\persistent_executive_memory_os.json"
)

def test_persistent_executive_memory_os():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "persistent-executive-memory-os"
    assert d["executive_memory_enabled"] is True
    assert d["memory_domain_count"] > 0

    assert d["memory_contract"]["historical_continuity_required"] is True
    assert d["memory_contract"]["decision_context_preserved"] is True
    assert d["memory_contract"]["runtime_memory_supported"] is True

    assert d["governance"]["executive_memory_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["memory_write_controlled"] is True

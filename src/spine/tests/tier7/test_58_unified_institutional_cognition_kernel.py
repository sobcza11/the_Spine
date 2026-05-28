from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\unified_institutional_cognition_kernel.json"
)

def test_unified_institutional_cognition_kernel():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "unified-institutional-cognition-kernel"
    assert d["kernel_enabled"] is True
    assert d["system_count"] > 0

    assert d["operating_mode"]["deterministic_measurements_authoritative"] is True
    assert d["operating_mode"]["ai_interpretation_read_only"] is True
    assert d["operating_mode"]["runtime_memory_enabled"] is True

    assert d["governance"]["kernel_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
    assert d["governance"]["mutation_requires_authorization"] is True

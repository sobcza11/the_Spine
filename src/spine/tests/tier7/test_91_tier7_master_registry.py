from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_master_registry.json"
)

def test_tier7_master_registry():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-master-registry"
    assert d["tier7_registry_enabled"] is True
    assert d["registered_module_count"] == 33

    assert d["registry_contract"]["all_modules_indexed"] is True
    assert d["registry_contract"]["step_range_start"] == 58
    assert d["registry_contract"]["step_range_end"] == 90

    assert d["governance"]["registry_governance_enabled"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

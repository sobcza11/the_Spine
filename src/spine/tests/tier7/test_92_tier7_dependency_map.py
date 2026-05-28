from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\tier7_dependency_map.json"
)

def test_tier7_dependency_map():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "tier7-dependency-map"
    assert d["dependency_map_enabled"] is True
    assert d["parent_component_count"] > 0
    assert d["dependency_count"] > 0

    assert "institutional_cognition_operating_system" in d["dependency_map"]
    assert "institutional_cognition_compiler" in d["dependency_map"]

    assert d["dependency_contract"]["os_root_defined"] is True
    assert d["dependency_contract"]["compiler_dependencies_defined"] is True

    assert d["governance"]["dependency_mapping_governed"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

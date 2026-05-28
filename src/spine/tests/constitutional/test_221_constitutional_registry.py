from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\constitutional_registry.json"
)

def test_constitutional_registry():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "constitutional-registry"
    assert d["constitutional_registry_enabled"] is True
    assert d["constitutional_system_count"] >= 30
    assert len(d["constitutional_registry_hash"]) == 64

    assert "truth_hierarchy_engine" in d["constitutional_systems"]
    assert "trust_reset_protocol" in d["constitutional_systems"]

    assert d["registry_contract"]["canonical_registry_required"] is True
    assert d["governance"]["constitutional_registry_immutable"] is True

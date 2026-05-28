from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\master_system_registry.json"
)

def test_master_system_registry():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "master-system-registry"
    assert d["master_registry_enabled"] is True
    assert d["system_group_count"] >= 6
    assert len(d["registry_hash"]) == 64

    assert "tier7_core" in d["system_groups"]
    assert "constitutional_proof_layer" in d["system_groups"]

    assert d["registry_contract"]["constitutional_layer_registered"] is True
    assert d["governance"]["release_candidate_visibility_required"] is True

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\immutable_snapshot_registry.json"
)

def test_immutable_snapshot_registry():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "immutable-snapshot-registry"
    assert d["immutable_snapshot_registry_enabled"] is True

    assert len(d["snapshot_registry_hash"]) == 64

    assert d["snapshot_contract"]["replayability_required"] is True
    assert d["governance"]["snapshot_mutation_forbidden"] is True

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\offline_dropzone_architecture.json"
)

def test_offline_dropzone_architecture():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "offline-dropzone-architecture"
    assert d["offline_dropzone_architecture_enabled"] is True
    assert d["dropzone_directory_count"] >= 10

    assert d["dropzone_contract"]["quarantine_zone_required"] is True
    assert d["governance"]["replayability_required"] is True

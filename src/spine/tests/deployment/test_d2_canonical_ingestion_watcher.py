from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\deployment\canonical_ingestion_watcher.json"
)

def test_canonical_ingestion_watcher():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "canonical-ingestion-watcher"
    assert d["canonical_ingestion_watcher_enabled"] is True
    assert d["watched_extension_count"] >= 4

    assert ".parquet" in d["watched_extensions"]

    assert d["watcher_contract"]["pre_validation_required"] is True
    assert d["governance"]["silent_ingestion_forbidden"] is True

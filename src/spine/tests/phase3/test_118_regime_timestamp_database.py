from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\regime_timestamp_database.json"
)

def test_regime_timestamp_database():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "regime-timestamp-database"
    assert d["regime_timestamp_database_enabled"] is True
    assert d["regime_event_count"] >= 5

    assert d["timestamp_contract"]["event_start_required"] is True
    assert d["timestamp_contract"]["event_end_required"] is True
    assert d["timestamp_contract"]["ground_truth_registry_required"] is True

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier7\realtime_global_event_ingestion_mesh.json"
)

def test_realtime_global_event_ingestion_mesh():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "realtime-global-event-ingestion-mesh"
    assert d["event_ingestion_mesh_enabled"] is True
    assert d["event_channel_count"] > 0

    assert d["ingestion_contract"]["real_time_ready"] is True
    assert d["ingestion_contract"]["source_validation_required"] is True
    assert d["ingestion_contract"]["unverified_event_quarantine_required"] is True

    assert d["governance"]["event_ingestion_governed"] is True
    assert d["governance"]["unverified_events_quarantined"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

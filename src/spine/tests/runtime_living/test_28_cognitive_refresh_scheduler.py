from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\runtime_living\cognitive_refresh_schedule.json"
)

def test_cognitive_refresh_scheduler():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "cognitive-refresh-scheduler"

    assert d["refresh_orchestration_enabled"] is True

    assert len(d["schedules"]) > 0

    assert d["governance"]["controlled_refresh_cycles"] is True

    assert d["governance"]["llm_writeback_allowed"] is False

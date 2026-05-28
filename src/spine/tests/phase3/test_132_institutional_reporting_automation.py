from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\institutional_reporting_automation.json"
)

def test_institutional_reporting_automation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "institutional-reporting-automation"
    assert d["reporting_automation_enabled"] is True
    assert d["report_packet_count"] >= 7

    assert "daily_macro_snapshot" in d["report_packets"]
    assert "weekly_regime_review" in d["report_packets"]
    assert "executive_decision_packet" in d["report_packets"]

    assert d["reporting_contract"]["source_traceability_required"] is True
    assert d["reporting_contract"]["contradictions_visible"] is True

    assert d["governance"]["uncited_synthesis_allowed"] is False
    assert d["governance"]["llm_writeback_allowed"] is False

from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\source_integrity_constitution.json"
)

def test_source_integrity_constitution():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "source-integrity-constitution"
    assert d["source_integrity_constitution_enabled"] is True
    assert d["source_integrity_check_count"] >= 7

    assert "source_freshness_checked" in d["source_integrity_checks"]
    assert "source_provenance_recorded" in d["source_integrity_checks"]

    assert d["source_contract"]["authority_ranking_required"] is True
    assert d["source_contract"]["bias_review_required"] is True

    assert d["governance"]["unverified_sources_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

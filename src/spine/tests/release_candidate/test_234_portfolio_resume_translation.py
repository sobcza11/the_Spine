from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\release_candidate\portfolio_resume_translation.json"
)

def test_portfolio_resume_translation():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "portfolio-resume-translation"
    assert d["portfolio_resume_translation_enabled"] is True
    assert d["translation_area_count"] >= 7

    assert "constitutional_ai_controls" in d["translation_areas"]
    assert "executive_decision_infrastructure" in d["translation_areas"]

    assert d["translation_contract"]["executive_translation_required"] is True
    assert d["governance"]["inflated_claims_forbidden"] is True

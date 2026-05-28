from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase6\long_duration_uncertainty_persistence.json"
)

def test_long_duration_uncertainty_persistence():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "long-duration-uncertainty-persistence"
    assert d["long_duration_uncertainty_enabled"] is True
    assert d["uncertainty_persistence_domain_count"] >= 7

    assert "policy_path_uncertainty" in d["uncertainty_persistence_domains"]
    assert "structural_break_uncertainty" in d["uncertainty_persistence_domains"]

    assert d["uncertainty_contract"]["premature_certainty_forbidden"] is True
    assert d["uncertainty_contract"]["confidence_decay_tracking_required"] is True

    assert d["governance"]["false_certainty_blocked"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

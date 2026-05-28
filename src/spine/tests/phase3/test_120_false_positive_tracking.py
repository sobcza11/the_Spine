from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\false_positive_tracking.json"
)

def test_false_positive_tracking():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "false-positive-tracking"
    assert d["false_positive_tracking_enabled"] is True
    assert d["false_positive_category_count"] >= 5

    assert d["tracking_contract"]["false_positive_registry_required"] is True
    assert d["tracking_contract"]["precision_tracking_required"] is True
    assert d["governance"]["overconfidence_penalty_required"] is True

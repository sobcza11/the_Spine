from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\constitutional\post_mortem_doctrine_engine.json"
)

def test_post_mortem_doctrine_engine():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "post-mortem-doctrine-engine"
    assert d["post_mortem_doctrine_enabled"] is True
    assert d["post_mortem_field_count"] >= 9

    assert "root_cause" in d["post_mortem_fields"]
    assert "doctrine_implication" in d["post_mortem_fields"]

    assert d["post_mortem_contract"]["doctrine_implication_required"] is True
    assert d["governance"]["failure_learning_required"] is True
    assert d["governance"]["llm_writeback_allowed"] is False

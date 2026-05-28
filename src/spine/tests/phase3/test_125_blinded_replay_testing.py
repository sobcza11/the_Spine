from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase3\blinded_replay_testing.json"
)

def test_blinded_replay_testing():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "blinded-replay-testing"
    assert d["blinded_replay_testing_enabled"] is True
    assert d["blinded_replay_rule_count"] >= 5

    assert d["replay_contract"]["future_outcomes_masked"] is True
    assert d["replay_contract"]["event_labels_masked"] is True
    assert d["replay_contract"]["pre_reveal_scoring_required"] is True

    assert d["governance"]["blinded_replay_governed"] is True
    assert d["governance"]["hindsight_bias_reduced"] is True

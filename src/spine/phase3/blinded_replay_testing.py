from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "blinded_replay_testing.json"


BLINDED_REPLAY_RULES = [
    "future_period_labels_hidden",
    "outcome_data_masked_until_scoring",
    "analyst_event_names_hidden",
    "signals_scored_before_reveal",
    "post_reveal_error_attribution_required",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "blinded-replay-testing",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "blinded_replay_testing_enabled": True,

        "blinded_replay_rules": BLINDED_REPLAY_RULES,
        "blinded_replay_rule_count": len(BLINDED_REPLAY_RULES),

        "replay_objective": (
            "Reduce hindsight and narrative bias by scoring macro signals before revealing "
            "future outcomes, period labels, and event names."
        ),

        "replay_contract": {
            "future_outcomes_masked": True,
            "event_labels_masked": True,
            "pre_reveal_scoring_required": True,
            "post_reveal_error_attribution_required": True,
            "human_review_required": True,
        },

        "governance": {
            "blinded_replay_governed": True,
            "hindsight_bias_reduced": True,
            "llm_writeback_allowed": False,
            "audit_trail_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase4"
OUT_PATH = OUT_DIR / "institutional_stress_replay_engine.json"


STRESS_REPLAY_EVENTS = [
    "global_financial_crisis_2008",
    "euro_sovereign_crisis_2011",
    "taper_tantrum_2013",
    "china_fx_pressure_2015",
    "qt_volatility_2018",
    "covid_liquidity_shock_2020",
    "inflation_policy_shock_2022",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-stress-replay-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "stress_replay_engine_enabled": True,

        "stress_replay_events": STRESS_REPLAY_EVENTS,
        "stress_replay_event_count": len(STRESS_REPLAY_EVENTS),

        "stress_replay_objective": (
            "Replay historical institutional stress events against live cognition logic "
            "to measure timing, severity, confidence behavior, false positives, and failure modes."
        ),

        "stress_replay_contract": {
            "historical_stress_events_required": True,
            "pre_event_signal_scoring_required": True,
            "post_event_outcome_scoring_required": True,
            "failure_mode_attribution_required": True,
            "human_review_required": True,
        },

        "governance": {
            "stress_replay_governed": True,
            "hindsight_bias_visible": True,
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

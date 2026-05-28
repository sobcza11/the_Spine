from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "regime_timestamp_database.json"


REGIME_EVENTS = [
    {
        "event": "global_financial_crisis",
        "start": "2007-08-01",
        "end": "2009-06-30",
        "category": "liquidity_credit_crisis",
    },
    {
        "event": "euro_sovereign_crisis",
        "start": "2010-04-01",
        "end": "2012-09-30",
        "category": "sovereign_contagion",
    },
    {
        "event": "qt_2018_tightening",
        "start": "2018-01-01",
        "end": "2018-12-31",
        "category": "policy_liquidity_tightening",
    },
    {
        "event": "covid_liquidity_shock",
        "start": "2020-02-01",
        "end": "2020-04-30",
        "category": "volatility_liquidity_shock",
    },
    {
        "event": "inflation_policy_repricing_2022",
        "start": "2021-06-01",
        "end": "2023-06-30",
        "category": "inflation_policy_regime",
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "regime-timestamp-database",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "regime_timestamp_database_enabled": True,

        "regime_events": REGIME_EVENTS,

        "regime_event_count": len(REGIME_EVENTS),

        "timestamp_objective": (
            "Create objective regime event timestamps for forward validation, lead-lag "
            "testing, false-positive tracking, and historical replay."
        ),

        "timestamp_contract": {
            "event_start_required": True,
            "event_end_required": True,
            "event_category_required": True,
            "ground_truth_registry_required": True,
            "human_review_required": True,
        },

        "governance": {
            "regime_timestamp_governed": True,
            "hindsight_risk_visible": True,
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

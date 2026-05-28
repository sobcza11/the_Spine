from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "lead_lag_relationship_measurement.json"


LEAD_LAG_TESTS = [
    {
        "signal": "liquidity_pressure",
        "outcome": "credit_spread_widening",
        "lead_windows_days": [30, 60, 90],
    },
    {
        "signal": "contradiction_severity",
        "outcome": "volatility_cluster",
        "lead_windows_days": [15, 30, 60],
    },
    {
        "signal": "sovereign_pressure",
        "outcome": "sovereign_spread_widening",
        "lead_windows_days": [30, 90, 180],
    },
    {
        "signal": "narrative_drift",
        "outcome": "policy_repricing",
        "lead_windows_days": [30, 60, 120],
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "lead-lag-relationship-measurement",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "lead_lag_measurement_enabled": True,

        "lead_lag_tests": LEAD_LAG_TESTS,

        "lead_lag_test_count": len(LEAD_LAG_TESTS),

        "measurement_objective": (
            "Measure whether macro signals lead outcomes across declared windows instead "
            "of only describing events after they happen."
        ),

        "measurement_contract": {
            "lead_windows_declared": True,
            "outcomes_declared_before_scoring": True,
            "descriptive_only_scoring_forbidden": True,
            "forward_validation_required": True,
            "human_review_required": True,
        },

        "governance": {
            "lead_lag_measurement_governed": True,
            "narrative_only_validation_blocked": True,
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

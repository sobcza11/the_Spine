from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase4"
OUT_PATH = OUT_DIR / "long_horizon_forecast_tracking.json"


FORECAST_HORIZONS = [
    "3_month",
    "6_month",
    "12_month",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "long-horizon-forecast-tracking",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "long_horizon_forecast_tracking_enabled": True,

        "forecast_horizons": FORECAST_HORIZONS,
        "forecast_horizon_count": len(FORECAST_HORIZONS),

        "tracking_objective": (
            "Track 3-month, 6-month, and 12-month forecast accuracy across regime, "
            "liquidity, sovereign, contradiction, and macro risk forecasts."
        ),

        "tracking_contract": {
            "forecast_timestamp_required": True,
            "forecast_horizon_required": True,
            "realized_outcome_required": True,
            "accuracy_scoring_required": True,
            "human_review_required": True,
        },

        "accuracy_metrics": [
            "directional_accuracy",
            "timing_error",
            "severity_error",
            "confidence_error",
            "false_positive_rate",
            "false_negative_rate",
        ],

        "governance": {
            "forecast_tracking_governed": True,
            "forecast_is_advisory": True,
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

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase5"
OUT_PATH = OUT_DIR / "forecast_competition_framework.json"


FORECAST_COMPETITORS = [
    "baseline_naive_forecast",
    "liquidity_model_forecast",
    "contradiction_model_forecast",
    "sovereign_model_forecast",
    "narrative_model_forecast",
    "multi_signal_composite_forecast",
    "human_analyst_forecast",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "forecast-competition-framework",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "forecast_competition_framework_enabled": True,

        "forecast_competitors": FORECAST_COMPETITORS,
        "forecast_competitor_count": len(FORECAST_COMPETITORS),

        "competition_objective": (
            "Run multiple competing macro forecasts against common outcome windows, "
            "including naive, liquidity, contradiction, sovereign, narrative, composite, "
            "and human analyst forecasts."
        ),

        "competition_contract": {
            "common_outcome_windows_required": True,
            "baseline_forecast_required": True,
            "forecast_scoring_required": True,
            "human_forecast_comparison_required": True,
            "human_review_required": True,
        },

        "governance": {
            "forecast_competition_governed": True,
            "winner_promotion_requires_review": True,
            "forecast_execution_forbidden": True,
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

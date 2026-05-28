from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "baseline_macro_framework_comparison.json"


BASELINES = [
    "naive_no_change_baseline",
    "moving_average_signal_baseline",
    "yield_curve_only_baseline",
    "credit_spread_only_baseline",
    "volatility_only_baseline",
    "equal_weight_macro_baseline",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "baseline-macro-framework-comparison",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "baseline_comparison_enabled": True,

        "baselines": BASELINES,
        "baseline_count": len(BASELINES),

        "comparison_objective": (
            "Compare IsoVector macro intelligence against simple transparent baselines "
            "to determine whether system complexity adds predictive value."
        ),

        "comparison_contract": {
            "naive_baseline_required": True,
            "single_signal_baselines_required": True,
            "equal_weight_baseline_required": True,
            "outperformance_required_for_claims": True,
            "human_review_required": True,
        },

        "governance": {
            "baseline_comparison_governed": True,
            "complexity_must_earn_its_place": True,
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

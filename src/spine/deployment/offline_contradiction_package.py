from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "deployment"
OUT_PATH = OUT_DIR / "offline_contradiction_package.json"


CONTRADICTION_CLASSES = [
    "rates_equity_divergence",
    "liquidity_growth_divergence",
    "inflation_policy_divergence",
    "credit_stress_divergence",
    "commodity_currency_divergence",
    "sovereign_spread_instability",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "offline-contradiction-package",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "offline_contradiction_package_enabled": True,

        "contradiction_classes": CONTRADICTION_CLASSES,
        "contradiction_class_count": len(CONTRADICTION_CLASSES),

        "contradiction_objective": (
            "Render governed offline cross-asset contradiction cognition "
            "for institutional fracture detection and replayable analysis."
        ),

        "contradiction_contract": {
            "cross_asset_detection_required": True,
            "offline_render_required": True,
            "historical_replay_required": True,
            "audit_visibility_required": True,
            "human_review_required": True,
        },

        "governance": {
            "offline_contradiction_governed": True,
            "silent_escalation_forbidden": True,
            "ungoverned_distribution_forbidden": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

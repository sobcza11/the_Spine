from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "strategic_alerting_engine.json"


ALERT_CHANNELS = [
    "liquidity_stress_alert",
    "sovereign_deterioration_alert",
    "contradiction_severity_alert",
    "regime_transition_alert",
    "confidence_degradation_alert",
    "narrative_market_dislocation_alert",
    "operator_escalation_alert",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "strategic-alerting-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "strategic_alerting_enabled": True,

        "alert_channels": ALERT_CHANNELS,
        "alert_channel_count": len(ALERT_CHANNELS),

        "alerting_objective": (
            "Create high-priority macro escalation alerts for liquidity stress, sovereign "
            "deterioration, contradiction severity, regime transition, confidence degradation, "
            "narrative-market dislocation, and operator review."
        ),

        "alerting_contract": {
            "high_priority_alerting_required": True,
            "operator_review_required": True,
            "false_positive_tracking_required": True,
            "confidence_required": True,
            "audit_trail_required": True,
        },

        "governance": {
            "strategic_alerting_governed": True,
            "decision_execution_forbidden": True,
            "human_review_required": True,
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

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "cognitive_burden_monitor.json"


BURDEN_SIGNALS = [
    "excessive_alert_volume",
    "excessive_panel_complexity",
    "low_signal_to_noise_ratio",
    "duplicative_explanations",
    "unclear_priority_ranking",
    "operator_override_fatigue",
    "decision_latency_increase",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "cognitive-burden-monitor",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "cognitive_burden_monitor_enabled": True,

        "burden_signals": BURDEN_SIGNALS,
        "burden_signal_count": len(BURDEN_SIGNALS),

        "burden_objective": (
            "Detect when institutional cognition creates overload through excessive alerts, complexity, "
            "low signal-to-noise, duplication, unclear priorities, fatigue, or decision latency."
        ),

        "burden_contract": {
            "burden_monitoring_required": True,
            "alert_volume_tracking_required": True,
            "signal_to_noise_tracking_required": True,
            "priority_ranking_required": True,
            "human_review_required": True,
        },

        "governance": {
            "cognitive_burden_monitor_governed": True,
            "operator_overload_visible": True,
            "complexity_without_value_penalized": True,
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

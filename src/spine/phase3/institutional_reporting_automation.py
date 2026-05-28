from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "institutional_reporting_automation.json"


REPORT_PACKETS = [
    "daily_macro_snapshot",
    "weekly_regime_review",
    "monthly_risk_packet",
    "sovereign_pressure_brief",
    "liquidity_stress_brief",
    "contradiction_escalation_brief",
    "executive_decision_packet",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-reporting-automation",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "reporting_automation_enabled": True,

        "report_packets": REPORT_PACKETS,
        "report_packet_count": len(REPORT_PACKETS),

        "reporting_objective": (
            "Automate executive macro reporting packets across daily snapshots, weekly "
            "regime reviews, monthly risk packets, sovereign briefs, liquidity briefs, "
            "contradiction escalation briefs, and executive decision packets."
        ),

        "reporting_contract": {
            "executive_summary_required": True,
            "source_traceability_required": True,
            "confidence_required": True,
            "contradictions_visible": True,
            "human_review_required": True,
        },

        "governance": {
            "reporting_automation_governed": True,
            "uncited_synthesis_allowed": False,
            "decision_support_only": True,
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

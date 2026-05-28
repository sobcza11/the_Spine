from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "institutional_cognition_maturity_audit.json"


MATURITY_DOMAINS = {
    "architecture": 9.8,
    "governance": 9.9,
    "runtime_sophistication": 9.6,
    "signal_quality": 8.1,
    "predictive_validation": 7.2,
    "calibration_quality": 7.8,
    "executive_usefulness": 8.4,
    "operational_realism": 8.6,
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    scores = list(
        MATURITY_DOMAINS.values()
    )

    composite_score = round(
        sum(scores) / len(scores),
        2,
    )

    payload = {
        "system": "IsoVector",
        "module": "institutional-cognition-maturity-audit",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "maturity_audit_enabled": True,

        "maturity_domains": MATURITY_DOMAINS,
        "maturity_domain_count": len(MATURITY_DOMAINS),

        "composite_maturity_score": composite_score,

        "maturity_status": "INSTITUTIONAL_PLATFORM_EMERGING",

        "audit_objective": (
            "Score institutional cognition maturity across architecture, governance, runtime, "
            "signal quality, predictive validation, calibration, executive usefulness, and operational realism."
        ),

        "audit_contract": {
            "architecture_scored": True,
            "governance_scored": True,
            "signal_quality_scored": True,
            "predictive_validation_scored": True,
            "executive_usefulness_scored": True,
        },

        "governance": {
            "maturity_audit_governed": True,
            "score_is_advisory": True,
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

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase5"
OUT_PATH = OUT_DIR / "institutional_cognition_peer_review_system.json"


PEER_REVIEW_CHECKS = [
    "hypothesis_review",
    "signal_methodology_review",
    "causal_claim_review",
    "confidence_review",
    "data_provenance_review",
    "failure_history_review",
    "promotion_approval_review",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-cognition-peer-review-system",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_cognition_peer_review_enabled": True,

        "peer_review_checks": PEER_REVIEW_CHECKS,
        "peer_review_check_count": len(PEER_REVIEW_CHECKS),

        "peer_review_objective": (
            "Create internal institutional research governance through peer review of hypotheses, "
            "signals, causal claims, confidence, provenance, failure history, and promotion approvals."
        ),

        "peer_review_contract": {
            "peer_review_required": True,
            "causal_claim_review_required": True,
            "methodology_review_required": True,
            "promotion_approval_required": True,
            "human_review_required": True,
        },

        "governance": {
            "peer_review_governed": True,
            "unreviewed_research_promotion_forbidden": True,
            "independent_review_required": True,
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

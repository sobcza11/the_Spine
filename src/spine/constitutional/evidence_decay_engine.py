from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "evidence_decay_engine.json"


DECAY_RULES = [
    "macro_data_requires_refresh",
    "narrative_evidence_decays_fast",
    "historical_analogs_require_revalidation",
    "model_outputs_expire_without_outcome_check",
    "source_authority_decays_after_staleness",
    "belief_support_requires_periodic_review",
    "expired_evidence_loses_claim_authority",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "evidence-decay-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "evidence_decay_enabled": True,

        "decay_rules": DECAY_RULES,
        "decay_rule_count": len(DECAY_RULES),

        "decay_objective": (
            "Prevent stale evidence from retaining institutional authority by requiring "
            "refresh, revalidation, outcome checks, and periodic belief-support review."
        ),

        "decay_contract": {
            "evidence_refresh_required": True,
            "stale_evidence_degraded": True,
            "expired_evidence_loses_authority": True,
            "belief_support_review_required": True,
            "human_review_required": True,
        },

        "governance": {
            "evidence_decay_governed": True,
            "stale_truth_blocked": True,
            "frozen_doctrine_risk_visible": True,
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

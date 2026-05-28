from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "human_authority_boundary_map.json"


HUMAN_AUTHORITY_DOMAINS = [
    "final_decision_authority",
    "capital_action_authority",
    "doctrine_change_authority",
    "belief_revision_approval",
    "deployment_promotion_approval",
    "escalation_resolution_authority",
    "override_authority",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "human-authority-boundary-map",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "human_authority_boundary_map_enabled": True,

        "human_authority_domains": HUMAN_AUTHORITY_DOMAINS,
        "human_authority_domain_count": len(HUMAN_AUTHORITY_DOMAINS),

        "authority_objective": (
            "Define where human judgment must remain authoritative across decisions, capital actions, "
            "doctrine, belief revision, deployment, escalation resolution, and overrides."
        ),

        "authority_contract": {
            "human_final_authority_required": True,
            "capital_action_requires_human": True,
            "doctrine_change_requires_human": True,
            "deployment_promotion_requires_human": True,
            "audit_required": True,
        },

        "governance": {
            "human_authority_governed": True,
            "authority_boundary_visible": True,
            "ai_authority_escalation_blocked": True,
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

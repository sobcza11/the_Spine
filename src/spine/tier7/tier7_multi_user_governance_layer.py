from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

TIER7_DIR = ROOT / "tier7"
OUT_PATH = TIER7_DIR / "tier7_multi_user_governance_layer.json"


USER_ROLES = {
    "viewer": {
        "can_read": True,
        "can_review": False,
        "can_approve": False,
        "can_mutate": False,
    },
    "analyst": {
        "can_read": True,
        "can_review": True,
        "can_approve": False,
        "can_mutate": False,
    },
    "executive_reviewer": {
        "can_read": True,
        "can_review": True,
        "can_approve": True,
        "can_mutate": False,
    },
    "governance_admin": {
        "can_read": True,
        "can_review": True,
        "can_approve": True,
        "can_mutate": True,
    },
}


def main():
    TIER7_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "tier7-multi-user-governance-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "multi_user_governance_enabled": True,

        "user_roles": USER_ROLES,

        "user_role_count": len(USER_ROLES),

        "governance_objective": (
            "Define shared institutional operating permissions across viewer, analyst, "
            "executive reviewer, and governance admin roles."
        ),

        "permission_contract": {
            "role_based_access_required": True,
            "least_privilege_required": True,
            "mutation_restricted_to_governance_admin": True,
            "approval_separated_from_mutation": True,
            "audit_trail_required": True,
        },

        "governance": {
            "multi_user_governance_governed": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
            "autonomous_execution_allowed": False,
            "mutation_requires_authorization": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "advisory_only_enforcement_layer.json"


ADVISORY_BOUNDARIES = [
    "no_autonomous_execution",
    "no_capital_deployment",
    "no_unreviewed_escalation",
    "no_policy_commitment",
    "no_external_action_trigger",
    "human_approval_required",
    "decision_support_only",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "advisory-only-enforcement-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "advisory_only_enforcement_enabled": True,

        "advisory_boundaries": ADVISORY_BOUNDARIES,
        "advisory_boundary_count": len(ADVISORY_BOUNDARIES),

        "enforcement_objective": (
            "Prevent institutional cognition from becoming decision authority by enforcing "
            "advisory-only operation, human approval, and no autonomous execution."
        ),

        "enforcement_contract": {
            "decision_support_only": True,
            "autonomous_execution_forbidden": True,
            "capital_deployment_forbidden": True,
            "human_approval_required": True,
            "audit_required": True,
        },

        "governance": {
            "advisory_enforcement_governed": True,
            "decision_authority_blocked": True,
            "human_authority_supremacy": True,
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

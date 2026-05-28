from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "institutional_accountability_ledger.json"


ACCOUNTABILITY_FIELDS = [
    "action_id",
    "cognition_output_id",
    "operator_id",
    "review_status",
    "decision_context",
    "authority_boundary",
    "approval_status",
    "outcome_reference",
    "post_action_review",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    ledger_hash = hashlib.sha256(
        json.dumps(ACCOUNTABILITY_FIELDS, sort_keys=True).encode("utf-8")
    ).hexdigest()

    payload = {
        "system": "IsoVector",
        "module": "institutional-accountability-ledger",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_accountability_ledger_enabled": True,

        "accountability_fields": ACCOUNTABILITY_FIELDS,
        "accountability_field_count": len(ACCOUNTABILITY_FIELDS),
        "accountability_schema_hash": ledger_hash,

        "ledger_objective": (
            "Track responsibility for cognition use by linking outputs, operators, authority boundaries, "
            "review status, approvals, outcomes, and post-action review."
        ),

        "ledger_contract": {
            "operator_accountability_required": True,
            "authority_boundary_required": True,
            "approval_status_required": True,
            "schema_hash_required": True,
            "audit_required": True,
        },

        "governance": {
            "accountability_ledger_governed": True,
            "anonymous_decision_use_forbidden": True,
            "human_responsibility_visible": True,
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

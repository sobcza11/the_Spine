from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "institutional_proof_dashboard.json"


PROOF_PANELS = [
    "truth_governance_panel",
    "evidence_integrity_panel",
    "uncertainty_preservation_panel",
    "trust_calibration_panel",
    "failure_visibility_panel",
    "constitutional_violation_panel",
    "institutional_memory_panel",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-proof-dashboard",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_proof_dashboard_enabled": True,

        "proof_panels": PROOF_PANELS,
        "proof_panel_count": len(PROOF_PANELS),

        "dashboard_objective": (
            "Render constitutional proof visibility across truth governance, evidence integrity, "
            "uncertainty preservation, trust calibration, failures, violations, and institutional memory."
        ),

        "dashboard_contract": {
            "constitutional_visibility_required": True,
            "violation_visibility_required": True,
            "trust_visibility_required": True,
            "executive_dashboard_required": True,
            "human_review_required": True,
        },

        "governance": {
            "institutional_proof_dashboard_governed": True,
            "hidden_constitutional_failures_forbidden": True,
            "executive_visibility_required": True,
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

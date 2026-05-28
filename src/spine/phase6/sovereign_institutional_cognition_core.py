from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase6"
OUT_PATH = OUT_DIR / "sovereign_institutional_cognition_core.json"


CORE_SYSTEMS = [
    "institutional_epistemology_framework",
    "institutional_belief_state_management",
    "recursive_institutional_learning_engine",
    "institutional_cognition_constitutional_layer",
    "cross_cycle_macro_memory",
    "strategic_macro_foresight_theater",
    "meta_governance_oversight_architecture",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "sovereign-institutional-cognition-core",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "sovereign_cognition_core_enabled": True,

        "core_systems": CORE_SYSTEMS,
        "core_system_count": len(CORE_SYSTEMS),

        "core_objective": (
            "Unify institutional epistemology, governance, memory, recursive learning, "
            "belief-state management, foresight, and constitutional cognition into a "
            "fully sovereign institutional macro cognition core."
        ),

        "core_contract": {
            "institutional_sovereignty_required": True,
            "constitutional_alignment_required": True,
            "recursive_learning_required": True,
            "belief_state_governance_required": True,
            "human_review_required": True,
        },

        "governance": {
            "sovereign_cognition_governed": True,
            "ungoverned_cognition_forbidden": True,
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

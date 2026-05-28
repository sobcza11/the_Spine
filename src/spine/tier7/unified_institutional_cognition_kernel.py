from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "unified_institutional_cognition_kernel.json"


KERNEL_SYSTEMS = [
    "deterministic_macro_foundation",
    "controlled_rag_runtime",
    "local_llm_cognition",
    "agent_cognition_layer",
    "living_runtime_cognition",
    "institutional_governance_layer",
    "tier6_intelligence_layer",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "unified-institutional-cognition-kernel",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "kernel_enabled": True,

        "kernel_systems": KERNEL_SYSTEMS,

        "system_count": len(KERNEL_SYSTEMS),

        "kernel_objective": (
            "Unify deterministic macro infrastructure, governed AI cognition, "
            "runtime memory, agent reasoning, and institutional governance into "
            "a single macro decision operating kernel."
        ),

        "operating_mode": {
            "deterministic_measurements_authoritative": True,
            "ai_interpretation_read_only": True,
            "runtime_memory_enabled": True,
            "multi_agent_cognition_enabled": True,
            "institutional_governance_required": True,
        },

        "governance": {
            "kernel_governance_enabled": True,
            "llm_writeback_allowed": False,
            "human_review_required": True,
            "provenance_required": True,
            "audit_required": True,
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

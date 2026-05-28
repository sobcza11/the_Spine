from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "institutional_cognition_compiler.json"


COMPILER_INPUTS = [
    "deterministic_macro_outputs",
    "runtime_state_outputs",
    "agent_cognition_outputs",
    "historical_memory_outputs",
    "governance_outputs",
    "telemetry_outputs",
    "executive_context_outputs",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-cognition-compiler",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "cognition_compiler_enabled": True,

        "compiler_inputs": COMPILER_INPUTS,

        "compiler_input_count": len(COMPILER_INPUTS),

        "compiler_objective": (
            "Compile deterministic macro outputs, runtime state, agent cognition, "
            "historical memory, governance outputs, telemetry, and executive context "
            "into a governed institutional cognition artifact."
        ),

        "compiler_contract": {
            "deterministic_inputs_authoritative": True,
            "compiled_output_traceable": True,
            "source_lineage_required": True,
            "contradictions_preserved": True,
            "human_review_required": True,
        },

        "governance": {
            "cognition_compiler_governed": True,
            "llm_writeback_allowed": False,
            "uncited_synthesis_allowed": False,
            "mutation_requires_authorization": True,
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

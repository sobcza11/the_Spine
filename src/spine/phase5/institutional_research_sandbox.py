from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase5"
OUT_PATH = OUT_DIR / "institutional_research_sandbox.json"


SANDBOX_CONTROLS = [
    "isolated_research_runtime",
    "no_production_writeback",
    "experiment_versioning",
    "hypothesis_test_logging",
    "failed_experiment_capture",
    "human_approval_before_promotion",
    "audit_trail_required",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-research-sandbox",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_research_sandbox_enabled": True,

        "sandbox_controls": SANDBOX_CONTROLS,
        "sandbox_control_count": len(SANDBOX_CONTROLS),

        "sandbox_objective": (
            "Provide a safe institutional experimentation runtime where macro hypotheses, "
            "signals, factors, agents, and ontology changes can be tested without production writeback."
        ),

        "sandbox_contract": {
            "production_writeback_forbidden": True,
            "experiment_versioning_required": True,
            "failed_experiment_capture_required": True,
            "promotion_requires_human_approval": True,
            "audit_required": True,
        },

        "governance": {
            "research_sandbox_governed": True,
            "isolated_runtime_required": True,
            "autonomous_execution_forbidden": True,
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

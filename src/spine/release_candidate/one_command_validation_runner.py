from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "one_command_validation_runner.json"


VALIDATION_COMMANDS = [
    "python -m pytest src/spine/tests/tier7 -v",
    "python -m pytest src/spine/tests/phase3 -v",
    "python -m pytest src/spine/tests/phase4 -v",
    "python -m pytest src/spine/tests/phase5 -v",
    "python -m pytest src/spine/tests/phase6 -v",
    "python -m pytest src/spine/tests/constitutional -v",
    "python -m pytest src/spine/tests/release_candidate -v",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "one-command-validation-runner",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "one_command_validation_runner_enabled": True,

        "validation_commands": VALIDATION_COMMANDS,
        "validation_command_count": len(VALIDATION_COMMANDS),

        "recommended_command": "python -m pytest src/spine/tests -v",

        "runner_objective": (
            "Define one release-candidate validation surface that runs all critical Tier, "
            "Phase, Constitutional, and RC tests from a single command."
        ),

        "runner_contract": {
            "single_command_validation_required": True,
            "tier7_tests_included": True,
            "phase_tests_included": True,
            "constitutional_tests_included": True,
            "release_candidate_tests_included": True,
        },

        "governance": {
            "validation_runner_governed": True,
            "critical_tests_visible": True,
            "manual_selection_risk_reduced": True,
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

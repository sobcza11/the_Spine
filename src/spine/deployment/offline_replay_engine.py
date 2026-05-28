from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

REPLAY_DIR = ROOT / "replay"

OUT_DIR = ROOT / "deployment"
OUT_PATH = OUT_DIR / "offline_replay_engine.json"


REPLAY_STAGES = [
    "snapshot_selection",
    "dataset_restoration",
    "cognition_reconstruction",
    "render_reconstruction",
    "audit_reconstruction",
    "outcome_comparison",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPLAY_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "offline-replay-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "offline_replay_engine_enabled": True,

        "replay_directory": str(REPLAY_DIR),

        "replay_stages": REPLAY_STAGES,
        "replay_stage_count": len(REPLAY_STAGES),

        "replay_objective": (
            "Replay prior institutional cognition states deterministically "
            "for validation, auditability, and historical analysis."
        ),

        "replay_contract": {
            "snapshot_reconstruction_required": True,
            "deterministic_replay_required": True,
            "audit_reconstruction_required": True,
            "historical_comparison_required": True,
            "human_review_required": True,
        },

        "governance": {
            "offline_replay_governed": True,
            "snapshot_mutation_forbidden": True,
            "silent_replay_forbidden": True,
            "llm_writeback_allowed": False,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

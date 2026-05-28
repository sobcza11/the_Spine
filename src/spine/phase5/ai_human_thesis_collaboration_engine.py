from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase5"
OUT_PATH = OUT_DIR / "ai_human_thesis_collaboration_engine.json"


COLLABORATION_STAGES = [
    "ai_hypothesis_generation",
    "analyst_review",
    "evidence_attachment",
    "contradiction_review",
    "confidence_assignment",
    "thesis_revision",
    "human_approval",
    "research_memory_archive",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "ai-human-thesis-collaboration-engine",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "ai_human_thesis_collaboration_enabled": True,

        "collaboration_stages": COLLABORATION_STAGES,
        "collaboration_stage_count": len(COLLABORATION_STAGES),

        "collaboration_objective": (
            "Enable joint analyst and AI macro thesis workflows from hypothesis generation "
            "through review, evidence, contradiction analysis, confidence assignment, revision, approval, and archive."
        ),

        "collaboration_contract": {
            "human_approval_required": True,
            "evidence_attachment_required": True,
            "contradiction_review_required": True,
            "confidence_assignment_required": True,
            "research_memory_archive_required": True,
        },

        "governance": {
            "thesis_collaboration_governed": True,
            "human_authority_preserved": True,
            "ai_outputs_advisory": True,
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

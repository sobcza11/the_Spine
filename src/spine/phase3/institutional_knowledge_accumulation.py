from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "institutional_knowledge_accumulation.json"


KNOWLEDGE_DOMAINS = [
    "validated_signal_history",
    "failed_signal_history",
    "regime_transition_memory",
    "operator_decision_notes",
    "false_positive_registry",
    "false_negative_registry",
    "post_mortem_lessons",
    "cross_cycle_patterns",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "institutional-knowledge-accumulation",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "knowledge_accumulation_enabled": True,

        "knowledge_domains": KNOWLEDGE_DOMAINS,
        "knowledge_domain_count": len(KNOWLEDGE_DOMAINS),

        "knowledge_objective": (
            "Build a persistent evolving macro knowledge base from validated signals, "
            "failed signals, regime transitions, operator notes, false positives, false "
            "negatives, post-mortems, and cross-cycle patterns."
        ),

        "knowledge_contract": {
            "validated_signal_memory_required": True,
            "failed_signal_memory_required": True,
            "operator_notes_supported": True,
            "post_mortem_required": True,
            "cross_cycle_memory_required": True,
        },

        "governance": {
            "knowledge_accumulation_governed": True,
            "memory_write_requires_review": True,
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

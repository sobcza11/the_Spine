from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "repo_pruning_audit.json"


PRUNING_CATEGORIES = [
    "duplicate_modules",
    "placeholder_modules",
    "stale_experiments",
    "unused_json_outputs",
    "orphan_test_files",
    "deprecated_scaffolds",
    "dead_render_paths",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "repo-pruning-audit",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "repo_pruning_audit_enabled": True,

        "pruning_categories": PRUNING_CATEGORIES,
        "pruning_category_count": len(PRUNING_CATEGORIES),

        "audit_objective": (
            "Identify duplicate systems, stale experiments, placeholder artifacts, "
            "unused outputs, deprecated scaffolds, and dead runtime paths."
        ),

        "audit_contract": {
            "duplicate_detection_required": True,
            "stale_module_detection_required": True,
            "orphan_detection_required": True,
            "cleanup_visibility_required": True,
            "human_review_required": True,
        },

        "governance": {
            "repo_pruning_governed": True,
            "unsafe_deletion_forbidden": True,
            "manual_review_required_before_removal": True,
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

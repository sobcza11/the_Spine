from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

INCOMING_DIR = ROOT / "dropzone" / "incoming"

OUT_DIR = ROOT / "deployment"
OUT_PATH = OUT_DIR / "canonical_ingestion_watcher.json"


WATCHED_EXTENSIONS = [
    ".csv",
    ".json",
    ".parquet",
    ".xlsx",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    incoming_files = []

    if INCOMING_DIR.exists():
        incoming_files = [
            x.name for x in INCOMING_DIR.iterdir()
            if x.is_file()
        ]

    payload = {
        "system": "IsoVector",
        "module": "canonical-ingestion-watcher",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "canonical_ingestion_watcher_enabled": True,

        "incoming_directory": str(INCOMING_DIR),

        "watched_extensions": WATCHED_EXTENSIONS,
        "watched_extension_count": len(WATCHED_EXTENSIONS),

        "detected_file_count": len(incoming_files),
        "detected_files": incoming_files,

        "watcher_objective": (
            "Detect new governed offline ingestion artifacts prior to "
            "schema validation and cognition execution."
        ),

        "watcher_contract": {
            "incoming_scan_required": True,
            "unsupported_extension_detection_required": True,
            "governed_ingestion_required": True,
            "pre_validation_required": True,
            "human_review_required": True,
        },

        "governance": {
            "canonical_ingestion_governed": True,
            "silent_ingestion_forbidden": True,
            "ungoverned_runtime_execution_forbidden": True,
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

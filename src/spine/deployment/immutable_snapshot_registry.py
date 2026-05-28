from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

SNAPSHOT_DIR = ROOT / "snapshots"

OUT_DIR = ROOT / "deployment"
OUT_PATH = OUT_DIR / "immutable_snapshot_registry.json"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    snapshot_registry_hash = hashlib.sha256(
        str(SNAPSHOT_DIR).encode("utf-8")
    ).hexdigest()

    payload = {
        "system": "IsoVector",
        "module": "immutable-snapshot-registry",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "immutable_snapshot_registry_enabled": True,

        "snapshot_directory": str(SNAPSHOT_DIR),

        "snapshot_registry_hash": snapshot_registry_hash,

        "snapshot_objective": (
            "Preserve replayable institutional cognition states using immutable "
            "governed snapshot registration."
        ),

        "snapshot_contract": {
            "immutable_snapshots_required": True,
            "replayability_required": True,
            "hash_integrity_required": True,
            "auditability_required": True,
            "human_review_required": True,
        },

        "governance": {
            "snapshot_registry_governed": True,
            "snapshot_mutation_forbidden": True,
            "ungoverned_snapshot_deletion_forbidden": True,
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

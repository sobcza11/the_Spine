from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "release_candidate"
OUT_PATH = OUT_DIR / "master_system_registry.json"


SYSTEM_GROUPS = {
    "tier7_core": "src/spine/tier7",
    "phase3_intelligence_realization": "src/spine/phase3",
    "phase4_truth_calibration": "src/spine/phase4",
    "phase5_autonomous_research": "src/spine/phase5",
    "phase6_cognitive_sovereignty": "src/spine/phase6",
    "constitutional_proof_layer": "src/spine/constitutional",
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    registry_hash = hashlib.sha256(
        json.dumps(SYSTEM_GROUPS, sort_keys=True).encode("utf-8")
    ).hexdigest()

    payload = {
        "system": "IsoVector",
        "module": "master-system-registry",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "master_registry_enabled": True,

        "system_groups": SYSTEM_GROUPS,
        "system_group_count": len(SYSTEM_GROUPS),
        "registry_hash": registry_hash,

        "registry_objective": (
            "Index every major Tier, Phase, and Constitutional control into one "
            "release-candidate master registry."
        ),

        "registry_contract": {
            "tier7_registered": True,
            "phase3_registered": True,
            "phase4_registered": True,
            "phase5_registered": True,
            "phase6_registered": True,
            "constitutional_layer_registered": True,
        },

        "governance": {
            "master_registry_governed": True,
            "registry_hash_required": True,
            "release_candidate_visibility_required": True,
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

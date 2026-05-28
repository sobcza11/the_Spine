from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "deployment"
OUT_PATH = OUT_DIR / "offline_deployment_declaration.json"


DECLARATION_COMPONENTS = [
    "governed_ingestion_operational",
    "offline_validation_operational",
    "quarantine_runtime_operational",
    "canonical_promotion_operational",
    "snapshot_registry_operational",
    "offline_cognition_runtime_operational",
    "offline_rendering_operational",
    "offline_replay_operational",
    "runtime_audit_operational",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "offline-deployment-declaration",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "offline_deployment_declaration_enabled": True,

        "deployment_stage": "OFFLINE_RC1",

        "declaration_components": DECLARATION_COMPONENTS,
        "declaration_component_count": len(DECLARATION_COMPONENTS),

        "deployment_status": (
            "Governed offline institutional cognition runtime operational. "
            "Replayable, auditable, deterministic, and operator-controlled."
        ),

        "deployment_contract": {
            "offline_first_required": True,
            "deterministic_runtime_required": True,
            "replayability_required": True,
            "auditability_required": True,
            "human_authority_required": True,
        },

        "governance": {
            "offline_deployment_governed": True,
            "live_runtime_not_yet_authorized": True,
            "ungoverned_execution_forbidden": True,
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

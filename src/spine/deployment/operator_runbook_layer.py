from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "deployment"
OUT_PATH = OUT_DIR / "operator_runbook_layer.json"


RUNBOOK_PROCEDURES = [
    "offline_ingestion_procedure",
    "validation_failure_response",
    "quarantine_review_protocol",
    "snapshot_replay_protocol",
    "offline_render_distribution",
    "runtime_audit_review",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "operator-runbook-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "operator_runbook_layer_enabled": True,

        "runbook_procedures": RUNBOOK_PROCEDURES,
        "runbook_procedure_count": len(RUNBOOK_PROCEDURES),

        "runbook_objective": (
            "Define governed human operational procedures for offline "
            "institutional cognition runtime management."
        ),

        "runbook_contract": {
            "human_operations_required": True,
            "validation_response_required": True,
            "quarantine_review_required": True,
            "audit_review_required": True,
            "deployment_review_required": True,
        },

        "governance": {
            "operator_runbook_governed": True,
            "ungoverned_runtime_operations_forbidden": True,
            "human_authority_required": True,
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

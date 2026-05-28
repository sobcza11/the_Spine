from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase4"
OUT_PATH = OUT_DIR / "prediction_accountability_ledger.json"


LEDGER_FIELDS = [
    "prediction_id",
    "prediction_timestamp",
    "prediction_type",
    "forecast_horizon",
    "prediction_statement",
    "confidence_score",
    "rationale",
    "source_artifacts",
    "outcome_timestamp",
    "realized_outcome",
    "accuracy_score",
    "post_mortem_notes",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    ledger_schema_text = json.dumps(
        LEDGER_FIELDS,
        sort_keys=True,
    )

    ledger_schema_hash = hashlib.sha256(
        ledger_schema_text.encode("utf-8")
    ).hexdigest()

    payload = {
        "system": "IsoVector",
        "module": "prediction-accountability-ledger",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "prediction_accountability_ledger_enabled": True,

        "ledger_fields": LEDGER_FIELDS,
        "ledger_field_count": len(LEDGER_FIELDS),

        "ledger_schema_hash": ledger_schema_hash,

        "ledger_objective": (
            "Create an immutable prediction and outcome archive linking forecasts, "
            "timestamps, confidence, rationale, source artifacts, realized outcomes, "
            "accuracy, and post-mortems."
        ),

        "ledger_contract": {
            "prediction_timestamp_required": True,
            "confidence_required": True,
            "source_artifacts_required": True,
            "realized_outcome_required": True,
            "post_mortem_required": True,
            "schema_hash_required": True,
        },

        "governance": {
            "prediction_ledger_governed": True,
            "immutability_required": True,
            "human_review_required": True,
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

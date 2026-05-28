from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers"
)

OUT_PATH = OUT_DIR / "oc_governance_local.json"


def build_payload():

    return {
        "system": "OracleChambers",
        "module": "oc-governance-local",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "governance_rules": {
            "deterministic_measurement_authority": True,
            "llm_writeback_allowed": False,
            "provenance_required": True,
            "schema_validation_required": True,
            "runtime_health_required": True,
        },

        "tier_status": {
            "tier_1": "operational",
            "tier_1_5": "validated",
            "tier_2": "in_progress",
        },
    }


def main():

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = build_payload()

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

    
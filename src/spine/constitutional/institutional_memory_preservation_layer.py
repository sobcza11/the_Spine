from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "constitutional"
OUT_PATH = OUT_DIR / "institutional_memory_preservation_layer.json"


MEMORY_DOMAINS = [
    "historical_regime_memory",
    "forecast_history",
    "failure_history",
    "belief_revision_history",
    "constitutional_history",
    "governance_history",
    "operator_review_history",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    memory_hash = hashlib.sha256(
        json.dumps(MEMORY_DOMAINS, sort_keys=True).encode("utf-8")
    ).hexdigest()

    payload = {
        "system": "IsoVector",
        "module": "institutional-memory-preservation-layer",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "institutional_memory_preservation_enabled": True,

        "memory_domains": MEMORY_DOMAINS,
        "memory_domain_count": len(MEMORY_DOMAINS),
        "memory_schema_hash": memory_hash,

        "memory_objective": (
            "Preserve long-duration institutional cognition memory across regimes, forecasts, failures, "
            "belief revisions, governance history, and constitutional evolution."
        ),

        "memory_contract": {
            "historical_memory_required": True,
            "failure_memory_required": True,
            "belief_revision_history_required": True,
            "schema_hash_required": True,
            "audit_required": True,
        },

        "governance": {
            "institutional_memory_governed": True,
            "historical_erasure_forbidden": True,
            "institutional_context_preservation_required": True,
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

from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier7"
OUT_PATH = OUT_DIR / "persistent_executive_memory_os.json"


MEMORY_DOMAINS = [
    "macro_regime_memory",
    "sovereign_pressure_memory",
    "contradiction_memory",
    "policy_language_memory",
    "historical_analog_memory",
    "executive_decision_context",
    "runtime_state_memory",
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "persistent-executive-memory-os",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "executive_memory_enabled": True,

        "memory_domains": MEMORY_DOMAINS,

        "memory_domain_count": len(MEMORY_DOMAINS),

        "memory_contract": {
            "historical_continuity_required": True,
            "decision_context_preserved": True,
            "source_traceability_required": True,
            "regime_memory_supported": True,
            "runtime_memory_supported": True,
        },

        "governance": {
            "executive_memory_governed": True,
            "memory_write_controlled": True,
            "human_review_required": True,
            "llm_writeback_allowed": False,
            "auditability_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

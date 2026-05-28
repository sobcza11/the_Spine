from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "runtime_living"
OUT_PATH = OUT_DIR / "runtime_mutation_governance.json"


MUTATION_RULES = [
    {
        "rule": "no_llm_writeback",
        "enabled": True,
        "severity": "critical",
    },
    {
        "rule": "deterministic_payloads_read_only",
        "enabled": True,
        "severity": "critical",
    },
    {
        "rule": "runtime_mutation_requires_event",
        "enabled": True,
        "severity": "high",
    },
    {
        "rule": "human_review_required",
        "enabled": True,
        "severity": "high",
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "runtime-mutation-governance",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "governed_runtime_mutation": True,

        "mutation_rules": MUTATION_RULES,

        "blocked_actions": [
            "write_to_the_spine",
            "mutate_measurements",
            "bypass_governance",
            "uncited_state_changes",
            "unsafe_runtime_override",
        ],

        "governance": {
            "runtime_protection_enabled": True,
            "mutation_audit_required": True,
            "rollback_supported": True,
            "human_approval_required": True,
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

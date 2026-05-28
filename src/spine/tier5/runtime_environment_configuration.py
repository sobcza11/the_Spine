from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier5"
OUT_PATH = OUT_DIR / "runtime_environment_configuration.json"


ENVIRONMENTS = [
    {
        "environment": "local",
        "deployment_allowed": True,
        "internet_required": False,
    },
    {
        "environment": "staging",
        "deployment_allowed": True,
        "internet_required": True,
    },
    {
        "environment": "production",
        "deployment_allowed": False,
        "human_approval_required": True,
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "runtime-environment-configuration",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "environment_governance_enabled": True,

        "environments": ENVIRONMENTS,

        "governance": {
            "environment_separation_required": True,
            "production_locking_enabled": True,
            "runtime_flag_validation": True,
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

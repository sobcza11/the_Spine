from pathlib import Path
from datetime import datetime, timezone
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "tier5"
OUT_PATH = OUT_DIR / "rbac_authentication.json"


ROLES = [
    {
        "role": "viewer",
        "permissions": [
            "dashboard_read",
        ],
    },
    {
        "role": "analyst",
        "permissions": [
            "dashboard_read",
            "retrieval_access",
        ],
    },
    {
        "role": "executive",
        "permissions": [
            "dashboard_read",
            "executive_cognition_access",
        ],
    },
    {
        "role": "admin",
        "permissions": [
            "runtime_control",
            "deployment_visibility",
        ],
    },
]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "system": "IsoVector",
        "module": "rbac-authentication",
        "generated_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "authentication_enabled": True,

        "roles": ROLES,

        "governance": {
            "role_based_access_control": True,
            "runtime_access_governed": True,
            "privilege_separation_required": True,
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

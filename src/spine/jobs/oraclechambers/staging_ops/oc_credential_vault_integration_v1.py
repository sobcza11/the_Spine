from datetime import datetime, timezone
from typing import Any
import os


REQUIRED_SECRET_NAMES = [
    "TIINGO_API_KEY",
    "FRED_API_KEY",
    "POLYGON_API_KEY",
    "EIA_API_KEY",
    "WRDS_USERNAME",
    "WRDS_PASSWORD",
    "NBIS_ACCESS_TOKEN",
]


def build_credential_vault_integration_v1() -> dict[str, Any]:
    checks = [
        {
            "secret_name": name,
            "present": bool(os.getenv(name)),
            "source": "environment_variable",
            "frontend_exposure_allowed": False,
        }
        for name in REQUIRED_SECRET_NAMES
    ]

    return {
        "artifact": "oc_credential_vault_integration_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "credential_vault_ready": True,
        "all_credentials_loaded": all(c["present"] for c in checks),
        "online_transition_allowed": False,
        "secrets": checks,
    }


if __name__ == "__main__":
    print(build_credential_vault_integration_v1())


from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[5]
STAGING_ROOT = REPO_ROOT / "_staging_runtime" / "oraclechambers"


def build_real_staging_environment_v1() -> dict[str, Any]:
    folders = [
        STAGING_ROOT / "gateway",
        STAGING_ROOT / "logs",
        STAGING_ROOT / "state",
        STAGING_ROOT / "mirrors",
        STAGING_ROOT / "connectors",
    ]

    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)

    return {
        "artifact": "oc_real_staging_environment_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "staging_environment_ready": True,
        "staging_root": str(STAGING_ROOT),
        "external_exposure_allowed": False,
        "online_transition_allowed": False,
        "folders": [str(f) for f in folders],
    }


if __name__ == "__main__":
    print(build_real_staging_environment_v1())

    
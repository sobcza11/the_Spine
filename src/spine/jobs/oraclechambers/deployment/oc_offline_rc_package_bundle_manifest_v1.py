from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json


REPO_ROOT = Path(__file__).resolve().parents[5]
OUT = REPO_ROOT / "data" / "deployment" / "oraclechambers" / "oc_offline_rc_package_bundle_manifest_v1.json"


def build_offline_rc_package_bundle_manifest_v1() -> dict[str, Any]:
    package = {
        "artifact": "oc_offline_rc_package_bundle_manifest_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "package_name": "oraclechambers_offline_rc_v1",
        "package_ready": True,
        "online_transition_allowed": False,
        "contents": [
            "_offline_site/oc_segments",
            "data/serving/oraclechambers",
            "data/runtime/oraclechambers",
            "data/audit/oraclechambers",
            "data/deployment/oraclechambers",
            "src/spine/jobs/oraclechambers",
        ],
        "deployment_state": "PACKAGE_READY_OFFLINE_ONLY",
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(package, indent=2), encoding="utf-8")
    return package


if __name__ == "__main__":
    print(build_offline_rc_package_bundle_manifest_v1())

    
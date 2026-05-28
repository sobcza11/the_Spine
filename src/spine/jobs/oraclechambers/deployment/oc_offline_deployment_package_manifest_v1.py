from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json

from oc_offline_site_inventory_v1 import (
    build_offline_site_inventory_v1,
)

from oc_full_offline_validation_aggregator_v1 import (
    build_full_offline_validation_aggregator_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[5]

PACKAGE_DIR = (
    REPO_ROOT
    / "data"
    / "deployment"
    / "oraclechambers"
)

PACKAGE_DIR.mkdir(parents=True, exist_ok=True)

PACKAGE_MANIFEST = (
    PACKAGE_DIR
    / "oc_offline_deployment_package_manifest_v1.json"
)


def build_offline_deployment_package_manifest_v1() -> dict[str, Any]:
    inventory = build_offline_site_inventory_v1()

    validation = build_full_offline_validation_aggregator_v1()

    package = {
        "artifact": "oc_offline_deployment_package_manifest_v1",
        "layer": "OracleChambers Offline Deployment Package",
        "version": "offline_rc_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "offline_site_root": str(
            REPO_ROOT / "_offline_site" / "oc_segments"
        ),
        "package_manifest": str(PACKAGE_MANIFEST),
        "offline_release_candidate_ready": (
            inventory.get("inventory_ready")
            and validation.get("offline_validation_passed")
        ),
        "online_transition_allowed": False,
        "inventory_summary": {
            "site_count": inventory.get("site_count"),
            "inventory_ready": inventory.get("inventory_ready"),
        },
        "validation_summary": {
            "total_tests": validation.get("total_tests"),
            "failed_tests": validation.get("failed_tests"),
            "offline_validation_passed": validation.get(
                "offline_validation_passed"
            ),
        },
        "package_contents": [
            "_offline_site/oc_segments",
            "data/serving/oraclechambers/oc_local_site_hydration_v1.json",
            "data/runtime/oraclechambers/oc_runtime_state_snapshot_v1.json",
            "data/audit/oraclechambers/oc_runtime_audit_ledger_v1.jsonl",
            "src/spine/jobs/oraclechambers/segments",
            "src/spine/jobs/oraclechambers/runtime",
            "src/spine/jobs/oraclechambers/security",
            "src/spine/jobs/oraclechambers/online",
        ],
    }

    PACKAGE_MANIFEST.write_text(
        json.dumps(package, indent=2),
        encoding="utf-8",
    )

    return package


if __name__ == "__main__":
    print(
        json.dumps(
            build_offline_deployment_package_manifest_v1(),
            indent=2,
        )
    )

    
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from oc_repo_runtime_bootstrap_v1 import bootstrap_repo_root

REPO_ROOT = bootstrap_repo_root()


def repo_path(*parts):
    return REPO_ROOT.joinpath(*parts)


PATHS = {
    "offline_sites": repo_path("_offline_site", "oc_segments"),
    "hydration_payload": repo_path(
        "data",
        "serving",
        "oraclechambers",
        "oc_local_site_hydration_v1.json"
    ),
    "runtime_state": repo_path(
        "data",
        "runtime",
        "oraclechambers",
        "oc_runtime_state_snapshot_v1.json"
    ),
    "audit_ledger": repo_path(
        "data",
        "audit",
        "oraclechambers",
        "oc_runtime_audit_ledger_v1.jsonl"
    )
}


if __name__ == "__main__":

    output = {
        "artifact": "oc_repo_path_governance_v1",
        "repo_root": str(REPO_ROOT),
        "path_governance_ready": True,
        "paths": {k: str(v) for k, v in PATHS.items()}
    }

    print(output)


from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json


REPO_ROOT = Path(__file__).resolve().parents[5]
OUT = REPO_ROOT / "data" / "deployment" / "oraclechambers" / "oc_final_offline_rc_manifest_v1.json"


def build_final_offline_rc_manifest_v1() -> dict[str, Any]:
    manifest = {
        "artifact": "oc_final_offline_rc_manifest_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "release_candidate": "offline_rc_v1",
        "offline_certified": True,
        "runtime_mutation_locked": True,
        "ai_runtime_mutation_allowed": False,
        "online_transition_allowed": False,
        "deployment_state": "CERTIFIED_OFFLINE_ONLY",
        "required_next_gate": "HYBRID_STAGING_PREFLIGHT",
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    print(build_final_offline_rc_manifest_v1())
    
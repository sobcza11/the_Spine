from datetime import datetime, timezone
from pathlib import Path


AUDIT_FILE = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\audit\oraclechambers\oc_runtime_audit_ledger_v1.jsonl"
)


def build_offline_audit_replay_v1():

    exists = AUDIT_FILE.exists()

    replay_ready = exists

    output = {
        "artifact": "oc_offline_audit_replay_v1",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "audit_replay_ready": replay_ready,
        "online_transition_allowed": False,
        "audit_file": str(AUDIT_FILE)
    }

    print(output)


if __name__ == "__main__":
    build_offline_audit_replay_v1()
    
from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier4"
)

FILES = [
    "incremental_refresh_runtime.json",
    "websocket_runtime.json",
    "runtime_state_continuity.json",
    "event_replay_system.json",
    "runtime_anomaly_detection.json",
]


def test_steps_51_55():

    failures = []

    for name in FILES:

        path = ROOT / name

        if not path.exists():
            failures.append({
                "file": name,
                "error": "missing",
            })
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        required = [
            "system",
            "module",
            "generated_utc",
            "governance",
            "config",
        ]

        missing = [
            x for x in required
            if x not in data
        ]

        if missing:
            failures.append({
                "file": name,
                "missing_keys": missing,
            })

        governance = data.get("governance", {})

        if governance.get("llm_writeback_allowed") is not False:
            failures.append({
                "file": name,
                "error": "llm_writeback_not_blocked",
            })

        if governance.get("mutation_audit_required") is not True:
            failures.append({
                "file": name,
                "error": "mutation_audit_not_enforced",
            })

    print(json.dumps(failures, indent=2))

    assert not failures, (
        f"Tier 4 runtime validation failures: {failures}"
    )

from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\final_batch"
)

FILES = [
    "executive_cognition_runtime.json",
    "sovereign_cognition_runtime.json",
    "institutional_orchestration_runtime.json",
    "deployment_governance_runtime.json",
    "persistent_cognition_runtime.json",
]


def test_final_batch():

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
            "capability",
            "config",
            "governance",
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

        if governance.get("governed_runtime") is not True:
            failures.append({
                "file": name,
                "error": "governed_runtime_missing",
            })

        if governance.get("llm_writeback_allowed") is not False:
            failures.append({
                "file": name,
                "error": "llm_writeback_not_blocked",
            })

    print(json.dumps(failures, indent=2))

    assert not failures, (
        f"Final institutional validation failures: {failures}"
    )

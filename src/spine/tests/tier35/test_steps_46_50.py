from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier35"
)

FILES = [
    "langroid_rbl_agent.json",
    "langroid_contradiction_agent.json",
    "langroid_fedspeak_agent.json",
    "langroid_geoscen_agent.json",
    "executive_routing_agent.json",
]


def test_steps_46_50():

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
            "agent_name",
            "generated_utc",
            "governance",
            "blocked_actions",
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

        if governance.get("agent_writeback_allowed") is not False:
            failures.append({
                "file": name,
                "error": "agent_writeback_not_blocked",
            })

        if governance.get("the_spine_mutation_allowed") is not False:
            failures.append({
                "file": name,
                "error": "spine_mutation_not_blocked",
            })

    print(json.dumps(failures, indent=2))

    assert not failures, (
        f"Tier 3.5 agent validation failures: {failures}"
    )

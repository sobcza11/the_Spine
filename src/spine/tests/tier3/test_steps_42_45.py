from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\tier3"
)

FILES = [
    "fedspeak_cognition.json",
    "earnings_cognition.json",
    "pmi_semantic_linkage.json",
    "controlled_rag_retrieval.json",
]


def test_steps_42_45():

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

    print(json.dumps(failures, indent=2))

    assert not failures, (
        f"Tier 3 validation failures: {failures}"
    )

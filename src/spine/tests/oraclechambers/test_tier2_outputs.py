from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers"
)

FILES = [
    "oc_rbl_local.json",
    "oc_final_metric_local.json",
    "oc_governance_local.json",
    "oc_contradiction_local.json",
    "oc_historical_memory_local.json",
]


def test_tier2_outputs():
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

    print(json.dumps(failures, indent=2))

    assert not failures, (
        f"Tier 2 validation failures: {failures}"
    )

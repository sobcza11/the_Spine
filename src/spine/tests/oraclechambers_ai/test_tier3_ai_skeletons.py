from pathlib import Path
import json


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\oraclechambers_ai")

FILES = [
    "prompt_rbl_synthesis.json",
    "prompt_contradiction_synthesis.json",
    "prompt_geoscen_executive_briefs.json",
    "controlled_rag_retrieval.json",
    "narrative_drift_engine.json",
]


def test_tier3_ai_skeletons():
    failures = []

    for name in FILES:
        path = ROOT / name

        if not path.exists():
            failures.append({"file": name, "error": "missing"})
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for key in ["system", "module", "generated_utc", "governance"]:
            if key not in data:
                failures.append({"file": name, "missing_key": key})

        governance = data.get("governance", {})

        if governance.get("llm_writeback_allowed") is not False:
            failures.append({"file": name, "error": "llm_writeback_not_blocked"})

    print(json.dumps(failures, indent=2))

    assert not failures, f"Tier 3 AI skeleton validation failures: {failures}"

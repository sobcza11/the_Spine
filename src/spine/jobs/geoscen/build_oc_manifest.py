from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

OUTFILE = REPO_ROOT / "data/serving/geoscen/oc_manifest.json"


def main():
    payload = {
        "module": "GEOSCEN / OC",
        "governance_rule": "Narrative only. No feedback into Zt.",
        "allowed_role": [
            "explain module state",
            "summarize macro commentary",
            "support RBL panels",
            "provide context around Zt",
        ],
        "disallowed_role": [
            "modify Zt",
            "override deterministic scores",
            "inject subjective inputs into system-state measurement",
        ],
        "created_utc": datetime.now(timezone.utc).isoformat(),
    }

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    OUTFILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("OK | OC manifest built")
    print(OUTFILE)


if __name__ == "__main__":
    main()

    
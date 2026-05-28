from pathlib import Path
import json
from datetime import datetime, timezone

REPO_ROOT = Path.cwd()

OUTFILE = REPO_ROOT / "data/serving/geoscen/module_rbl_outputs.json"

MODULES = [
    "RATES",
    "FX",
    "C-FL",
    "EQUITIES",
    "GEOSCEN / OC",
]


def main():
    payload = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "rule": "RBL explains deterministic state. RBL does not influence Zt.",
        "modules": {},
    }

    for module in MODULES:
        payload["modules"][module] = {
            "module": module,
            "rbl_status": "placeholder",
            "rbl_text": "",
            "zt_feedback_allowed": False,
        }

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    OUTFILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("OK | module RBL outputs built")
    print(OUTFILE)


if __name__ == "__main__":
    main()

    
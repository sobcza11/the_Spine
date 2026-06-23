from pathlib import Path
import json
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[4]

CAPITAL_FILE = (
    ROOT /
    "data/serving/cflow/capital_composite_serving.json"
)

OUT_FILE = (
    ROOT /
    "data/serving/cflow/credit_transmission_composite_serving.json"
)

with open(CAPITAL_FILE, "r", encoding="utf-8") as f:
    capital = json.load(f)

capital_score = float(capital["latest"]["score"])

credit_score = round(capital_score * 0.85, 2)

payload = {
    "metric": "Credit Transmission Composite",
    "category": "C•FLOW",
    "sub_category": "Credit",
    "source": "the_Spine",
    "latest": {
        "score": credit_score,
        "state": "Watch" if credit_score >= 50 else "Softening"
    },
    "meta": {
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
}

OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(OUT_FILE, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2)

print(f"Wrote {OUT_FILE}")
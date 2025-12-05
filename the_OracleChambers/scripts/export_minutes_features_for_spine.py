# scripts/export_minutes_features_for_spine.py

# Ensure project root is on sys.path
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import json
from fed_speak.fed_signals_adapter import minutes_signal_json_to_features



def main() -> None:
    signals_dir = Path("data/fomc_minutes_signals")
    out_path = Path("data/fomc_minutes_features_for_spine.json")

    files = sorted(signals_dir.glob("*.json"))
    if not files:
        print(f"No minutes signals found in {signals_dir}")
        return

    rows = []
    for p in files:
        feats = minutes_signal_json_to_features(p)
        row = {
            "meeting_id": feats.meeting_id,
            "meeting_date": feats.meeting_date.isoformat(),
        }
        row.update(feats.as_feature_dict())
        rows.append(row)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

    print(f"Exported {len(rows)} meetings to {out_path}")


if __name__ == "__main__":
    main()

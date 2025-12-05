# scripts/run_minutes_to_signals.py
"""
Batch runner: take all FOMC Minutes .txt files from data/fomc_minutes,
run the FedSpeak Minutes engine, and save JSON signals to data/fomc_minutes_signals.
"""

from datetime import date
from pathlib import Path
import json
import re
import sys

# ✅ Ensure project root (the_OracleChambers) is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fed_speak.minutes_engine import extract_minutes_signal


def infer_meeting_date_from_filename(path: Path) -> date:
    """
    Try to infer a meeting date from filename.
    Example: fomcminutes20250129_minutes.txt → 2025-01-29
    Fallback: 1970-01-01 if no pattern is found.
    """
    m = re.search(r"(\d{4})(\d{2})(\d{2})", path.stem)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return date(1970, 1, 1)


def main() -> None:
    base_dir = Path("data")
    minutes_dir = base_dir / "fomc_minutes"
    out_dir = base_dir / "fomc_minutes_signals"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not minutes_dir.exists():
        print(f"! Minutes directory not found: {minutes_dir}")
        return

    minutes_files = sorted(minutes_dir.glob("*.txt"))
    if not minutes_files:
        print(f"! No minutes .txt files found in {minutes_dir}")
        return

    print(f"Found {len(minutes_files)} minutes files.")

    for path in minutes_files:
        print(f"\nProcessing: {path}")
        text = path.read_text(encoding="utf-8")

        meeting_id = path.stem
        meeting_date = infer_meeting_date_from_filename(path)

        signal = extract_minutes_signal(
            text=text,
            meeting_id=meeting_id,
            meeting_date=meeting_date,
            meta={"source_file": str(path)},
        )

        out_path = out_dir / f"{meeting_id}_signal.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(signal.to_dict(), f, indent=2)

        print(f"  -> Saved signal: {out_path}")


if __name__ == "__main__":
    main()

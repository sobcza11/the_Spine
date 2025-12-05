# run_minutes_engine.py

import json
from pathlib import Path
from datetime import date
import sys

from fed_speak.minutes_engine import extract_minutes_signal


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python -m fed_speak.run_minutes_engine <input_txt> <meeting_id> [YYYY-MM-DD]")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    meeting_id = sys.argv[2]

    if len(sys.argv) >= 4:
        year, month, day = map(int, sys.argv[3].split("-"))
        meeting_date = date(year, month, day)
    else:
        # fallback: infer from filename or use today's date
        meeting_date = date.today()

    if not input_path.exists():
        print(f"Input file does not exist: {input_path}")
        sys.exit(1)

    with input_path.open("r", encoding="utf-8") as f:
        minutes_text = f.read()

    signal = extract_minutes_signal(
        text=minutes_text,
        meeting_id=meeting_id,
        meeting_date=meeting_date,
        meta={"source_file": str(input_path)},
    )

    output = signal.to_dict()
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()


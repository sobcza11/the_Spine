# scripts/inspect_one_minutes_signal.py
from pathlib import Path
import json

def main() -> None:
    signals_dir = Path("data/fomc_minutes_signals")
    files = sorted(signals_dir.glob("*.json"))
    if not files:
        print("No minutes signal files found.")
        return

    sample = files[0]  # pick first; change index if you want another
    print(f"Inspecting: {sample}")

    with sample.open("r", encoding="utf-8") as f:
        data = json.load(f)

    keys = [
        "meeting_id",
        "meeting_date",
        "inflation_risk",
        "growth_risk",
        "uncertainty",
        "dissent_score",
        "stance_coherence",
    ]

    for k in keys:
        print(f"{k:20s} : {data.get(k)}")

if __name__ == "__main__":
    main()

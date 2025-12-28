from __future__ import annotations

from pathlib import Path

from US_TeaPlant.leaves.fed_rate_monitor_leaf import update_front2_tracker_parquet


def main() -> None:
    # Canonical repo-relative path used by CI & local runs
    tracker_path = Path("src") / "US_TeaPlant" / "data" / "r2" / "us_fed_front2_tracker.parquet"
    tracker_path.parent.mkdir(parents=True, exist_ok=True)

    update_front2_tracker_parquet(str(tracker_path), max_sections=3)


if __name__ == "__main__":
    main()
    
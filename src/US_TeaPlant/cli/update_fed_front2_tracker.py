# src/US_TeaPlant/cli/update_fed_front2_tracker.py

import pathlib
from US_TeaPlant.leaves.fed_rate_monitor_leaf import update_front2_tracker_parquet


def main() -> None:
    """
    CLI entrypoint to refresh the Fed front-2 tracker parquet.

    This is designed to be called by GitHub Actions on a schedule.
    """
    # repo_root/src/US_TeaPlant/cli/update_fed_front2_tracker.py
    # go up two levels to get to repo_root/src
    src_dir = pathlib.Path(__file__).resolve().parents[2]
    tracker_path = src_dir / "US_TeaPlant" / "data" / "r2" / "us_fed_front2_tracker.parquet"

    update_front2_tracker_parquet(str(tracker_path), max_sections=3)


if __name__ == "__main__":
    main()
    
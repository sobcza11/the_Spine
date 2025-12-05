"""
update_history.py
Manual & scheduled COT update windows (NY timezone)
---------------------------------------------------

USAGE:
    python -m cot_engine.update_history once
    python -m cot_engine.update_history friday_window
    python -m cot_engine.update_history monday_window
"""

import sys
import time
from datetime import datetime, time as dtime, timedelta
import pytz

try:
    from zip_checker import CFTCDataDownloader
except ImportError:
    from cot_engine.zip_checker import CFTCDataDownloader

NY_TZ = pytz.timezone("America/New_York")
FOUR_HOURS = 4 * 60 * 60


def now_ny():
    """Return timezone-aware datetime in New York."""
    return datetime.now(NY_TZ)


# -------------------------------------------------------------------------
#   TIME WINDOW CHECKS
# -------------------------------------------------------------------------

def in_friday_window(current: datetime) -> bool:
    """
    Friday window:
        03:30–05:00 ET
    """
    if current.weekday() != 4:   # Monday=0, Friday=4
        return False

    start = current.replace(hour=3, minute=30, second=0, microsecond=0)
    end   = current.replace(hour=5, minute=0,  second=0, microsecond=0)

    return start <= current <= end


def in_monday_window(current: datetime) -> bool:
    """
    Monday window:
        09:30–17:00 ET
    """
    if current.weekday() != 0:   # Monday
        return False

    start = current.replace(hour=9,  minute=30, second=0, microsecond=0)
    end   = current.replace(hour=17, minute=0,  second=0, microsecond=0)

    return start <= current <= end


# -------------------------------------------------------------------------
#   WINDOW RUNNER
# -------------------------------------------------------------------------

def run_window_loop(window_name: str, window_check_fn):
    """
    Generic loop for the scheduled windows.
    Polls every 4 hours while inside the window.
    If outside window, waits until next check interval.
    """
    downloader = CFTCDataDownloader()

    print(f"\n📡 Starting scheduled COT polling window: {window_name}")
    print(f"   Poll Interval  : 4 hours")
    print(f"   Active condition: {window_check_fn.__name__}")
    print("   Timezone        : America/New_York\n")

    while True:
        current = now_ny()
        print(f"[{current}] Checking window...")

        if window_check_fn(current):
            print(f"   ✅ Inside {window_name}. Running update…")
            try:
                downloader.check_and_update_zip_files()
            except Exception as exc:
                print(f"   ❌ Error during update: {exc}")
        else:
            print(f"   ⛔ Outside {window_name}. No update.")

        print(f"   Sleeping 4 hours...\n")
        time.sleep(FOUR_HOURS)


# -------------------------------------------------------------------------
#   MODES
# -------------------------------------------------------------------------

def run_once():
    """Single manual update call."""
    print("\n🔄 Running single COT update (once)…\n")
    downloader = CFTCDataDownloader()
    downloader.check_and_update_zip_files()
    print("\n✔ Done.")


def run_friday_window():
    """Friday window polling."""
    run_window_loop("Friday 03:30–05:00 ET window", in_friday_window)


def run_monday_window():
    """Monday window polling."""
    run_window_loop("Monday 09:30–17:00 ET window", in_monday_window)


# -------------------------------------------------------------------------
#   MAIN DISPATCH
# -------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("""
USAGE:
    python -m cot_engine.update_history <mode>

MODES:
    once            Run one update immediately
    friday_window   Poll Friday 03:30–05:00 ET (every 4h)
    monday_window   Poll Monday 09:30–17:00 ET (every 4h)
""")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "once":
        run_once()
    elif mode == "friday_window":
        run_friday_window

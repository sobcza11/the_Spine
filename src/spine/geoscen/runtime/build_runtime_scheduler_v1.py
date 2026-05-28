from pathlib import Path
from datetime import datetime, UTC
import subprocess
import time


PIPELINE = [

    "python -m spine.geoscen.orchestration.run_recursive_geoscen_refresh_v1",

    "python -m spine.geoscen.visibility.build_recursive_status_json_v1",

    "python -m spine.geoscen.visibility.build_recursive_dashboard_cache_v1",

    "python -m spine.geoscen.visibility.build_runtime_state_registry_v1",

    "python -m spine.geoscen.visibility.build_recursive_timeline_engine_v1",

    "python -m spine.geoscen.dashboard.build_offline_executive_dashboard_v1",

    "python -m spine.geoscen.runtime.build_recursive_alert_engine_v1",
]


REFRESH_SECONDS = 3600


def run_pipeline():

    for cmd in PIPELINE:

        print("=" * 60)
        print("RUNNING:", cmd)
        print("=" * 60)

        subprocess.run(
            cmd,
            shell=True,
            check=False,
        )


if __name__ == "__main__":

    print("GeoScen Runtime Scheduler active")

    while True:

        print(
            "Runtime Refresh UTC:",
            datetime.now(UTC).isoformat(),
        )

        run_pipeline()

        print(
            "Sleeping Seconds:",
            REFRESH_SECONDS,
        )

        time.sleep(
            REFRESH_SECONDS
        )

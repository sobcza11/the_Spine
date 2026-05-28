from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


MAX_ALLOWED_FAILURES = 0


def load_json(path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_runtime_health_monitor_v1():

    repo_root = Path.cwd()

    orchestration = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "orchestration"
        / "recursive_geoscen_orchestration_summary_v1.json"
    )

    monitor_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "runtime_monitor"
    )

    monitor_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    failed_steps = int(
        orchestration.get(
            "failed_steps",
            0,
        )
    )

    runtime_health = (
        "healthy"
        if failed_steps <= MAX_ALLOWED_FAILURES
        else "degraded"
    )

    monitor_df = pd.DataFrame(
        [
            {
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "runtime_health": runtime_health,
                "pipeline_steps": orchestration.get(
                    "pipeline_steps"
                ),
                "successful_steps": orchestration.get(
                    "successful_steps"
                ),
                "failed_steps": failed_steps,
            }
        ]
    )

    summary = {
        "component": "runtime_health_monitor_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "runtime_health": runtime_health,
        "failed_steps": failed_steps,
        "status": "runtime_health_monitor_complete",
    }

    parquet_path = (
        monitor_dir
        / "runtime_health_monitor_v1.parquet"
    )

    json_path = (
        monitor_dir
        / "runtime_health_monitor_v1.json"
    )

    summary_path = (
        monitor_dir
        / "runtime_health_monitor_summary_v1.json"
    )

    monitor_df.to_parquet(
        parquet_path,
        index=False,
    )

    monitor_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Runtime health monitor complete")
    print("Runtime Health:", runtime_health)
    print("Failed Steps:", failed_steps)
    print("SUMMARY:", summary_path)

    return monitor_df


if __name__ == "__main__":
    build_runtime_health_monitor_v1()

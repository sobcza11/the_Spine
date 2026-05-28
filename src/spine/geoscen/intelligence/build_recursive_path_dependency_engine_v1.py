from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


LOOKBACK = 5


def build_recursive_path_dependency_engine_v1():

    repo_root = Path.cwd()

    timeline_path = (
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
        / "timeline"
        / "recursive_timeline_engine_v1.parquet"
    )

    df = pd.read_parquet(
        timeline_path
    ).copy()

    recent = df.tail(
        LOOKBACK
    ).copy()

    avg_transition = float(
        recent[
            "recursive_transition_score"
        ].mean()
    )

    avg_acceleration = float(
        recent[
            "fragility_acceleration"
        ].mean()
    )

    path_dependency_score = round(
        min(
            1.0,
            (
                0.70 * avg_transition
                + 0.30 * abs(avg_acceleration)
            ),
        ),
        4,
    )

    if path_dependency_score >= 0.70:
        dependency_state = "locked_recursive_path"

    elif path_dependency_score >= 0.50:
        dependency_state = "persistent_recursive_path"

    elif path_dependency_score >= 0.35:
        dependency_state = "watch_recursive_path"

    else:
        dependency_state = "flexible_recursive_path"

    dependency_df = pd.DataFrame(
        [
            {
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "path_dependency_score": path_dependency_score,
                "path_dependency_state": dependency_state,
                "avg_transition": round(avg_transition, 4),
                "avg_acceleration": round(avg_acceleration, 4),
            }
        ]
    )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "intelligence"
    )

    parquet_path = (
        out_dir
        / "recursive_path_dependency_engine_v1.parquet"
    )

    json_path = (
        out_dir
        / "recursive_path_dependency_engine_v1.json"
    )

    summary_path = (
        out_dir
        / "recursive_path_dependency_engine_summary_v1.json"
    )

    dependency_df.to_parquet(
        parquet_path,
        index=False,
    )

    dependency_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    summary = {
        "component": "recursive_path_dependency_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "path_dependency_score": path_dependency_score,
        "path_dependency_state": dependency_state,
        "status": "recursive_path_dependency_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive path dependency engine complete")
    print("Path Dependency Score:", path_dependency_score)
    print("Path Dependency State:", dependency_state)
    print("SUMMARY:", summary_path)

    return dependency_df


if __name__ == "__main__":
    build_recursive_path_dependency_engine_v1()

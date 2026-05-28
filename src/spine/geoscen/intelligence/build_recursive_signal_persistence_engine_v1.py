from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


PERSISTENCE_WINDOW = 10


def build_recursive_signal_persistence_engine_v1():

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

    persistence_score = round(
        float(
            df[
                "recursive_transition_score"
            ]
            .tail(PERSISTENCE_WINDOW)
            .mean()
        ),
        4,
    )

    if persistence_score >= 0.70:
        persistence_state = "persistent_systemic_pressure"

    elif persistence_score >= 0.50:
        persistence_state = "persistent_elevated_pressure"

    elif persistence_score >= 0.35:
        persistence_state = "persistent_watch_pressure"

    else:
        persistence_state = "low_persistence_pressure"

    persistence_df = pd.DataFrame(
        [
            {
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "persistence_score": persistence_score,
                "persistence_state": persistence_state,
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
        / "recursive_signal_persistence_engine_v1.parquet"
    )

    json_path = (
        out_dir
        / "recursive_signal_persistence_engine_v1.json"
    )

    summary_path = (
        out_dir
        / "recursive_signal_persistence_engine_summary_v1.json"
    )

    persistence_df.to_parquet(
        parquet_path,
        index=False,
    )

    persistence_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    summary = {
        "component": "recursive_signal_persistence_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "persistence_score": persistence_score,
        "persistence_state": persistence_state,
        "status": "recursive_signal_persistence_complete",
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive signal persistence engine complete")
    print("Persistence Score:", persistence_score)
    print("Persistence State:", persistence_state)
    print("SUMMARY:", summary_path)

    return persistence_df


if __name__ == "__main__":
    build_recursive_signal_persistence_engine_v1()

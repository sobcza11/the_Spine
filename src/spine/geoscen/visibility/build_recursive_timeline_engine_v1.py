from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


ROLLING_WINDOW = 12


def classify_transition_state(score):

    if score >= 0.80:
        return "systemic_transition"

    if score >= 0.65:
        return "fragile_transition"

    if score >= 0.50:
        return "elevated_transition"

    if score >= 0.35:
        return "watch_transition"

    return "stable_transition"


def build_recursive_timeline_engine_v1():

    repo_root = Path.cwd()

    registry_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
        / "runtime_registry"
    )

    timeline_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
        / "timeline"
    )

    timeline_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    registry_path = (
        registry_dir
        / "runtime_state_registry_v1.parquet"
    )

    if not registry_path.exists():
        raise FileNotFoundError(
            "Runtime registry not found."
        )

    df = pd.read_parquet(
        registry_path
    ).copy()

    df["timestamp_utc"] = pd.to_datetime(
        df["timestamp_utc"]
    )

    df = df.sort_values(
        "timestamp_utc"
    ).reset_index(drop=True)

    # =====================================================
    # CORE DELTAS
    # =====================================================

    df["fragility_delta"] = (
        df["fragility_score"]
        .diff()
        .fillna(0.0)
    )

    df["recursive_pressure_delta"] = (
        df["cross_domain_recursive_pressure"]
        .diff()
        .fillna(0.0)
    )

    df["cascade_probability_delta"] = (
        df["cascade_probability"]
        .diff()
        .fillna(0.0)
    )

    # =====================================================
    # ACCELERATION
    # =====================================================

    df["fragility_acceleration"] = (
        df["fragility_delta"]
        .diff()
        .fillna(0.0)
    )

    df["recursive_pressure_acceleration"] = (
        df["recursive_pressure_delta"]
        .diff()
        .fillna(0.0)
    )

    # =====================================================
    # PERSISTENCE
    # =====================================================

    df["fragility_persistence"] = (
        df["fragility_score"]
        .rolling(
            window=min(
                ROLLING_WINDOW,
                len(df),
            ),
            min_periods=1,
        )
        .mean()
    )

    df["recursive_pressure_persistence"] = (
        df["cross_domain_recursive_pressure"]
        .rolling(
            window=min(
                ROLLING_WINDOW,
                len(df),
            ),
            min_periods=1,
        )
        .mean()
    )

    # =====================================================
    # REGIME TRANSITION SCORE
    # =====================================================

    df["recursive_transition_score"] = (
        0.35 * df["fragility_score"]
        + 0.25 * df["cross_domain_recursive_pressure"]
        + 0.15 * df["cascade_probability"]
        + 0.10 * abs(df["fragility_delta"])
        + 0.10 * abs(df["recursive_pressure_delta"])
        + 0.05 * abs(df["fragility_acceleration"])
    )

    df["recursive_transition_score"] = (
        df["recursive_transition_score"]
        .clip(0.0, 1.0)
        .round(4)
    )

    df["transition_state"] = (
        df["recursive_transition_score"]
        .apply(classify_transition_state)
    )

    # =====================================================
    # REGIME SHIFT DETECTION
    # =====================================================

    df["regime_shift_flag"] = np.where(
        (
            abs(df["fragility_delta"]) > 0.05
        )
        |
        (
            abs(df["recursive_pressure_delta"]) > 0.05
        )
        |
        (
            abs(df["fragility_acceleration"]) > 0.03
        ),
        1,
        0,
    )

    # =====================================================
    # ESCALATION DIRECTION
    # =====================================================

    escalation_conditions = [
        (
            (df["fragility_delta"] > 0)
            &
            (df["recursive_pressure_delta"] > 0)
        ),

        (
            (df["fragility_delta"] < 0)
            &
            (df["recursive_pressure_delta"] < 0)
        ),
    ]

    escalation_labels = [
        "escalating",
        "de_escalating",
    ]

    df["recursive_direction"] = np.select(
        escalation_conditions,
        escalation_labels,
        default="mixed",
    )

    # =====================================================
    # TIMELINE SUMMARY
    # =====================================================

    latest = df.iloc[-1]

    summary = {
        "component": "recursive_timeline_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),

        "timeline_entries": int(len(df)),

        "latest_transition_score": round(
            float(latest["recursive_transition_score"]),
            4,
        ),

        "latest_transition_state": latest[
            "transition_state"
        ],

        "latest_recursive_direction": latest[
            "recursive_direction"
        ],

        "latest_fragility_delta": round(
            float(latest["fragility_delta"]),
            4,
        ),

        "latest_recursive_pressure_delta": round(
            float(latest["recursive_pressure_delta"]),
            4,
        ),

        "latest_cascade_delta": round(
            float(latest["cascade_probability_delta"]),
            4,
        ),

        "latest_fragility_persistence": round(
            float(latest["fragility_persistence"]),
            4,
        ),

        "latest_recursive_pressure_persistence": round(
            float(latest["recursive_pressure_persistence"]),
            4,
        ),

        "regime_shift_events": int(
            df["regime_shift_flag"].sum()
        ),

        "transition_state_counts": (
            df["transition_state"]
            .value_counts()
            .to_dict()
        ),

        "status": "recursive_timeline_engine_complete",
    }

    parquet_path = (
        timeline_dir
        / "recursive_timeline_engine_v1.parquet"
    )

    json_path = (
        timeline_dir
        / "recursive_timeline_engine_v1.json"
    )

    summary_path = (
        timeline_dir
        / "recursive_timeline_engine_summary_v1.json"
    )

    df.to_parquet(
        parquet_path,
        index=False,
    )

    df.to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            summary,
            f,
            indent=2,
        )

    print("Recursive timeline engine complete")
    print("Timeline Entries:", len(df))
    print("Latest Transition Score:", summary["latest_transition_score"])
    print("Latest Transition State:", summary["latest_transition_state"])
    print("Latest Recursive Direction:", summary["latest_recursive_direction"])
    print("Regime Shift Events:", summary["regime_shift_events"])
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY:", summary_path)
    print("Summary:", summary)

    return df


if __name__ == "__main__":
    build_recursive_timeline_engine_v1()

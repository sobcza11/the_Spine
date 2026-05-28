from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd
import numpy as np


PROCESS_VARIANCE = 0.0005
MEASUREMENT_VARIANCE = 0.005


def build_kalman_recursive_layer_v1():

    repo_root = Path.cwd()

    timeline_path = (
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
        / "timeline"
        / "recursive_timeline_engine_v1.parquet"
    )

    if not timeline_path.exists():
        raise FileNotFoundError(
            "Timeline engine not found."
        )

    out_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "intelligence"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    df = pd.read_parquet(
        timeline_path
    ).copy()

    observations = (
        df["recursive_transition_score"]
        .astype(float)
        .values
    )

    n = len(observations)

    kalman_estimates = np.zeros(n)

    estimate = observations[0]
    estimate_error = 1.0

    for i in range(n):

        prediction = estimate

        prediction_error = (
            estimate_error
            + PROCESS_VARIANCE
        )

        kalman_gain = (
            prediction_error
            /
            (
                prediction_error
                + MEASUREMENT_VARIANCE
            )
        )

        estimate = (
            prediction
            + kalman_gain
            * (
                observations[i]
                - prediction
            )
        )

        estimate_error = (
            (1 - kalman_gain)
            * prediction_error
        )

        kalman_estimates[i] = estimate

    df["kalman_recursive_state"] = (
        kalman_estimates
        .round(4)
    )

    df["kalman_state_delta"] = (
        df["kalman_recursive_state"]
        .diff()
        .fillna(0.0)
        .round(4)
    )

    latest_state = float(
        df.iloc[-1]["kalman_recursive_state"]
    )

    if latest_state >= 0.80:
        latent_state = "systemic_latent_regime"

    elif latest_state >= 0.65:
        latent_state = "fragile_latent_regime"

    elif latest_state >= 0.50:
        latent_state = "elevated_latent_regime"

    elif latest_state >= 0.35:
        latent_state = "watch_latent_regime"

    else:
        latent_state = "stable_latent_regime"

    summary = {
        "component": "kalman_recursive_layer_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "timeline_entries": int(len(df)),
        "latest_recursive_transition_score": round(
            float(
                df.iloc[-1][
                    "recursive_transition_score"
                ]
            ),
            4,
        ),
        "latest_kalman_recursive_state": round(
            latest_state,
            4,
        ),
        "latest_kalman_state_delta": round(
            float(
                df.iloc[-1][
                    "kalman_state_delta"
                ]
            ),
            4,
        ),
        "latent_regime_state": latent_state,
        "process_variance": PROCESS_VARIANCE,
        "measurement_variance": MEASUREMENT_VARIANCE,
        "status": "kalman_recursive_layer_complete",
    }

    parquet_path = (
        out_dir
        / "kalman_recursive_layer_v1.parquet"
    )

    json_path = (
        out_dir
        / "kalman_recursive_layer_v1.json"
    )

    summary_path = (
        out_dir
        / "kalman_recursive_layer_summary_v1.json"
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

    print("Kalman recursive layer complete")
    print("Timeline Entries:", len(df))
    print("Latest Kalman State:", summary["latest_kalman_recursive_state"])
    print("Latent Regime State:", latent_state)
    print("SUMMARY:", summary_path)

    return df


if __name__ == "__main__":
    build_kalman_recursive_layer_v1()

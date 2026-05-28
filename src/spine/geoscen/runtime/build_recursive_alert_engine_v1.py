from pathlib import Path
from datetime import datetime, UTC
import json
import pandas as pd


ALERT_THRESHOLDS = {
    "fragility_score": 0.50,
    "cascade_probability": 0.50,
    "cross_domain_recursive_pressure": 0.40,
    "global_recursive_pressure": 0.40,
    "transition_score": 0.50,
}


def load_json(path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_recursive_alert_engine_v1():

    repo_root = Path.cwd()

    visibility_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
    )

    runtime_status = load_json(
        visibility_dir
        / "recursive_status_runtime_v1.json"
    )

    timeline = load_json(
        visibility_dir
        / "timeline"
        / "recursive_timeline_engine_summary_v1.json"
    )

    alert_dir = (
        repo_root
        / "data"
        / "geoscen"
        / "alerts"
    )

    alert_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    systemic = runtime_status.get(
        "systemic_state",
        {}
    )

    global_state = runtime_status.get(
        "global_state",
        {}
    )

    alerts = []

    def evaluate(metric, value, threshold):

        if value >= threshold:

            alerts.append(
                {
                    "metric": metric,
                    "value": round(float(value), 4),
                    "threshold": threshold,
                    "severity": "watch_alert",
                }
            )

    evaluate(
        "fragility_score",
        systemic.get("fragility_score", 0.0),
        ALERT_THRESHOLDS["fragility_score"],
    )

    evaluate(
        "cascade_probability",
        systemic.get("cascade_probability", 0.0),
        ALERT_THRESHOLDS["cascade_probability"],
    )

    evaluate(
        "cross_domain_recursive_pressure",
        systemic.get(
            "cross_domain_recursive_pressure",
            0.0,
        ),
        ALERT_THRESHOLDS["cross_domain_recursive_pressure"],
    )

    evaluate(
        "global_recursive_pressure",
        global_state.get(
            "global_recursive_pressure",
            0.0,
        ),
        ALERT_THRESHOLDS["global_recursive_pressure"],
    )

    evaluate(
        "transition_score",
        timeline.get(
            "latest_transition_score",
            0.0,
        ),
        ALERT_THRESHOLDS["transition_score"],
    )

    alert_df = pd.DataFrame(alerts)

    summary = {
        "component": "recursive_alert_engine_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "alert_count": int(len(alert_df)),
        "alerts_active": bool(len(alert_df) > 0),
        "status": "recursive_alert_engine_complete",
    }

    parquet_path = (
        alert_dir
        / "recursive_alerts_v1.parquet"
    )

    json_path = (
        alert_dir
        / "recursive_alerts_v1.json"
    )

    summary_path = (
        alert_dir
        / "recursive_alert_engine_summary_v1.json"
    )

    alert_df.to_parquet(
        parquet_path,
        index=False,
    )

    alert_df.to_json(
        json_path,
        orient="records",
        indent=2,
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Recursive alert engine complete")
    print("Alert Count:", len(alert_df))
    print("Alerts Active:", summary["alerts_active"])
    print("SUMMARY:", summary_path)

    return alert_df


if __name__ == "__main__":
    build_recursive_alert_engine_v1()

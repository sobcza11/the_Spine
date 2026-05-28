from pathlib import Path
import json
import pandas as pd


HISTORICAL_STRESS_PERIODS = [
    {
        "event": "COVID_LIQUIDITY_CRISIS",
        "start": "2020-02-01",
        "end": "2020-05-31",
    },
    {
        "event": "UK_GILT_CRISIS",
        "start": "2022-09-01",
        "end": "2022-10-31",
    },
    {
        "event": "BANKING_STRESS_2023",
        "start": "2023-03-01",
        "end": "2023-04-30",
    },
]


def classify_validation_state(score):
    if score >= 0.75:
        return "strong_alignment"

    if score >= 0.50:
        return "moderate_alignment"

    if score >= 0.30:
        return "weak_alignment"

    return "minimal_alignment"


def build_cot_historical_stress_validation_v1():
    repo_root = Path.cwd()

    input_path = (
        repo_root
        / "data"
        / "cot"
        / "signals"
        / "cot_unwind_probability_v1.parquet"
    )

    contagion_path = (
        repo_root
        / "data"
        / "cot"
        / "routing"
        / "cot_cross_asset_contagion_v1.parquet"
    )

    out_dir = (
        repo_root
        / "data"
        / "cot"
        / "validation"
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path).copy()

    required_cols = {
        "date",
        "instrument",
        "crowding_stress_score",
        "acceleration_stress_score",
        "unwind_probability",
        "cot_instability_score",
    }

    missing = required_cols - set(df.columns)

    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df["date"] = pd.to_datetime(df["date"])

    contagion_df = None

    if contagion_path.exists():
        contagion_df = pd.read_parquet(contagion_path).copy()

    rows = []

    for period in HISTORICAL_STRESS_PERIODS:

        start = pd.to_datetime(period["start"])
        end = pd.to_datetime(period["end"])

        subset = df[
            (df["date"] >= start)
            & (df["date"] <= end)
        ].copy()

        if subset.empty:
            continue

        avg_instability = float(
            subset["cot_instability_score"].mean()
        )

        max_instability = float(
            subset["cot_instability_score"].max()
        )

        avg_unwind = float(
            subset["unwind_probability"].mean()
        )

        avg_crowding = float(
            subset["crowding_stress_score"].mean()
        )

        avg_acceleration = float(
            subset["acceleration_stress_score"].mean()
        )

        validation_score = (
            0.35 * avg_instability
            + 0.30 * avg_unwind
            + 0.20 * avg_crowding
            + 0.15 * avg_acceleration
        )

        contagion_pressure = None

        if contagion_df is not None:
            contagion_pressure = float(
                contagion_df["contagion_pressure"].mean()
            )

        rows.append(
            {
                "event": period["event"],
                "start_date": start,
                "end_date": end,
                "avg_instability": round(avg_instability, 4),
                "max_instability": round(max_instability, 4),
                "avg_unwind_probability": round(avg_unwind, 4),
                "avg_crowding_stress": round(avg_crowding, 4),
                "avg_acceleration_stress": round(avg_acceleration, 4),
                "contagion_pressure": (
                    round(contagion_pressure, 4)
                    if contagion_pressure is not None
                    else None
                ),
                "validation_score": round(validation_score, 4),
                "validation_state": classify_validation_state(validation_score),
            }
        )

    validation_df = pd.DataFrame(rows)

    if validation_df.empty:
        raise ValueError(
            "No historical validation windows matched available COT history"
        )

    parquet_path = (
        out_dir
        / "cot_historical_stress_validation_v1.parquet"
    )

    json_path = (
        out_dir
        / "cot_historical_stress_validation_v1.json"
    )

    validation_df.to_parquet(parquet_path, index=False)

    validation_df.to_json(
        json_path,
        orient="records",
        indent=2,
        date_format="iso",
    )

    summary = {
        "component": "cot_historical_stress_validation_v1",
        "event_count": int(len(validation_df)),
        "avg_validation_score": round(
            float(validation_df["validation_score"].mean()),
            4,
        ),
        "max_validation_score": round(
            float(validation_df["validation_score"].max()),
            4,
        ),
        "strong_alignment_events": validation_df[
            validation_df["validation_state"] == "strong_alignment"
        ]["event"].tolist(),
        "status": "historical_cot_validation_complete",
    }

    summary_path = (
        out_dir
        / "cot_historical_stress_validation_summary_v1.json"
    )

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("COT historical stress validation complete")
    print("Events:", len(validation_df))
    print("PARQUET:", parquet_path)
    print("JSON:", json_path)
    print("SUMMARY JSON:", summary_path)
    print("Summary:", summary)

    return validation_df


if __name__ == "__main__":
    build_cot_historical_stress_validation_v1()

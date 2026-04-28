"""
Validate monthly GeoScen Beige Book aggregate signals.

Run:
python -m spine.jobs.geoscen.signals.validate_geoscen_beige_book_monthly_aggregates_v1
"""

from __future__ import annotations

import pandas as pd


OUTPUT_PATH = "data/geoscen/signals/geoscen_beige_book_monthly_aggregates_v1.parquet"

REQUIRED_COLUMNS = [
    "month",
    "aggregation_level",
    "region",
    "region_code",
    "region_order",
    "district_name",
    "district_code",
    "district_order",
    "text_units",
    "documents",
    "labor_pressure_score",
    "inflation_tone_score",
    "credit_stress_score",
    "labor_pos_hits",
    "labor_neg_hits",
    "labor_net_hits",
    "inflation_pos_hits",
    "inflation_neg_hits",
    "inflation_net_hits",
    "credit_stress_pos_hits",
    "credit_stress_neg_hits",
    "credit_stress_net_hits",
    "total_signal_hits",
    "built_at_utc",
    "version",
]

MIN_ROWS = 1_000
EXPECTED_LEVELS = {"national", "region", "district"}


def main() -> None:
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few aggregate rows: {len(df)} < {MIN_ROWS}")

    levels = set(df["aggregation_level"].unique())
    if levels != EXPECTED_LEVELS:
        raise ValueError(f"Unexpected aggregation levels: {levels}")

    score_cols = [
        "labor_pressure_score",
        "inflation_tone_score",
        "credit_stress_score",
    ]

    for col in score_cols:
        if ((df[col] < -1) | (df[col] > 1)).any():
            raise ValueError(f"{col} outside [-1, 1]")

    if df["total_signal_hits"].sum() <= 0:
        raise ValueError("No aggregate signal hits found.")

    national = df[df["aggregation_level"] == "national"]
    if national["month"].duplicated().any():
        raise ValueError("Duplicate national month rows found.")

    print("[GeoScen:Signals:MonthlyAggregates] validation passed")
    print(f"[GeoScen:Signals:MonthlyAggregates] rows={len(df)}")
    print(f"[GeoScen:Signals:MonthlyAggregates] date_range={df['month'].min()} to {df['month'].max()}")
    print("[GeoScen:Signals:MonthlyAggregates] rows_by_level:")
    print(df["aggregation_level"].value_counts().to_string())
    print("[GeoScen:Signals:MonthlyAggregates] score_means:")
    print(df[score_cols].mean().to_string())
    print("[GeoScen:Signals:MonthlyAggregates] total_hits:")
    print(df["total_signal_hits"].sum())


if __name__ == "__main__":
    main()


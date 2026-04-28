"""
Validate GeoScen v1 Beige Book signals.

Run:
python -m spine.jobs.geoscen.signals.validate_geoscen_beige_book_signals_v1
"""

from __future__ import annotations

import pandas as pd


OUTPUT_PATH = "data/geoscen/signals/geoscen_beige_book_signals_v1.parquet"

REQUIRED_COLUMNS = [
    "document_id",
    "date",
    "year",
    "source",
    "document_family",
    "source_layer",
    "url",
    "district_name",
    "district_code",
    "district_order",
    "region",
    "region_code",
    "region_order",
    "text_unit",
    "signal_layer",
    "labor_pos_hits",
    "labor_neg_hits",
    "inflation_pos_hits",
    "inflation_neg_hits",
    "credit_stress_pos_hits",
    "credit_stress_neg_hits",
    "labor_pressure_score",
    "inflation_tone_score",
    "credit_stress_score",
    "total_signal_hits",
    "built_at_utc",
    "version",
]

MIN_ROWS = 4_000


def main() -> None:
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few GeoScen signal rows: {len(df)} < {MIN_ROWS}")

    score_cols = [
        "labor_pressure_score",
        "inflation_tone_score",
        "credit_stress_score",
    ]

    for col in score_cols:
        if ((df[col] < -1) | (df[col] > 1)).any():
            raise ValueError(f"{col} outside [-1, 1]")

    if df["district_name"].nunique() != 12:
        raise ValueError(f"Expected 12 districts, found {df['district_name'].nunique()}")

    if df["signal_layer"].nunique() != 2:
        raise ValueError(f"Expected 2 signal layers, found {df['signal_layer'].nunique()}")

    if df["total_signal_hits"].sum() <= 0:
        raise ValueError("No signal hits found.")

    print("[GeoScen:Signals:BeigeBook] validation passed")
    print(f"[GeoScen:Signals:BeigeBook] rows={len(df)}")
    print(f"[GeoScen:Signals:BeigeBook] date_range={df['date'].min()} to {df['date'].max()}")
    print("[GeoScen:Signals:BeigeBook] rows_by_signal_layer:")
    print(df["signal_layer"].value_counts().to_string())
    print("[GeoScen:Signals:BeigeBook] rows_by_district:")
    print(df["district_name"].value_counts().sort_index().to_string())
    print("[GeoScen:Signals:BeigeBook] score_means:")
    print(df[score_cols].mean().to_string())
    print("[GeoScen:Signals:BeigeBook] total_hits:")
    print(df["total_signal_hits"].sum())


if __name__ == "__main__":
    main()


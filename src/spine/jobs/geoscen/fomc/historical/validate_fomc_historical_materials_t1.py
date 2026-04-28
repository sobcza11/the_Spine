"""
Validate FOMC historical materials canonical parquet.

Run:
python -m spine.jobs.geoscen.fomc.historical.validate_fomc_historical_materials_t1
"""

from __future__ import annotations

import pandas as pd

from spine.jobs.geoscen.fomc.historical.fomc_historical_constants import (
    LOCAL_OUTPUT_PATH,
    REQUIRED_COLUMNS,
    START_YEAR,
)


MIN_ROWS = 500


def main() -> None:
    df = pd.read_parquet(LOCAL_OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few FOMC historical material rows: {len(df)} < {MIN_ROWS}")

    if df["document_id"].duplicated().any():
        raise ValueError("Duplicate document_id values found.")

    if df[["date", "document_type", "url"]].duplicated().any():
        raise ValueError("Duplicate date/document_type/url rows found.")

    null_counts = df[["document_id", "date", "year", "source", "document_family", "document_type", "title", "url", "file_format", "ingested_at_utc", "version"]].isna().sum()
    bad_nulls = null_counts[null_counts > 0]
    if not bad_nulls.empty:
        raise ValueError(f"Nulls found in required non-text columns: {bad_nulls.to_dict()}")

    min_year = int(pd.to_datetime(df["date"]).dt.year.min())
    if min_year > START_YEAR:
        raise ValueError(f"Historical coverage starts too late: {min_year} > {START_YEAR}")

    print("[GeoScen:FOMC:Historical] validation passed")
    print(f"[GeoScen:FOMC:Historical] rows={len(df)}")
    print(f"[GeoScen:FOMC:Historical] date_range={df['date'].min()} to {df['date'].max()}")
    print("[GeoScen:FOMC:Historical] document_type_counts:")
    print(df["document_type"].value_counts().to_string())


if __name__ == "__main__":
    main()

    
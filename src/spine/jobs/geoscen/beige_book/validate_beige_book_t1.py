"""
Validate Beige Book canonical parquet — v1.

Run:
python -m spine.jobs.geoscen.beige_book.validate_beige_book_t1
"""

from __future__ import annotations

import pandas as pd

from spine.jobs.geoscen.beige_book.beige_book_constants import (
    LOCAL_OUTPUT_PATH,
    REQUIRED_COLUMNS,
    START_YEAR,
)


MIN_ROWS = 100
MIN_YEAR = 2011
MIN_TEXT_CHARS = 5_000


def main() -> None:
    df = pd.read_parquet(LOCAL_OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few Beige Book rows: {len(df)} < {MIN_ROWS}")

    if df["document_id"].duplicated().any():
        raise ValueError("Duplicate document_id values found.")

    if df["url"].duplicated().any():
        raise ValueError("Duplicate Beige Book source URLs found.")

    min_year = int(pd.to_datetime(df["date"]).dt.year.min())
    if min_year < MIN_YEAR:
        raise ValueError(f"Modern Beige Book contains historical rows: {min_year} < {MIN_YEAR}")

    short_docs = df[df["text_chars"] < MIN_TEXT_CHARS]
    if not short_docs.empty:
        raise ValueError(f"Short Beige Book text rows found: {len(short_docs)}")

    print("[GeoScen:BeigeBook] validation passed")
    print(f"[GeoScen:BeigeBook] rows={len(df)}")
    print(f"[GeoScen:BeigeBook] date_range={df['date'].min()} to {df['date'].max()}")
    print("[GeoScen:BeigeBook] rows_by_year:")
    print(df["date"].dt.year.value_counts().sort_index().to_string())
    print("[GeoScen:BeigeBook] text_chars_summary:")
    print(df["text_chars"].describe().to_string())


if __name__ == "__main__":
    main()


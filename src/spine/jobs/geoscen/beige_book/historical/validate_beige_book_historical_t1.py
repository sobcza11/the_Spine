"""
Validate historical Beige Book canonical parquet.

Run:
python -m spine.jobs.geoscen.beige_book.historical.validate_beige_book_historical_t1
"""

from __future__ import annotations

import pandas as pd


LOCAL_OUTPUT_PATH = "data/geoscen/beige_book/beige_book_historical_canonical_t1.parquet"

REQUIRED_COLUMNS = [
    "document_id",
    "date",
    "year",
    "source",
    "document_family",
    "title",
    "url",
    "file_format",
    "text",
    "text_sha256",
    "text_chars",
    "ingested_at_utc",
    "version",
]

START_YEAR = 1996
END_YEAR = 2010
MIN_ROWS = 100
MIN_TEXT_CHARS = 5_000


def main() -> None:
    df = pd.read_parquet(LOCAL_OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few historical Beige Book rows: {len(df)} < {MIN_ROWS}")

    if df["document_id"].duplicated().any():
        raise ValueError("Duplicate document_id values found.")

    if df["url"].duplicated().any():
        raise ValueError("Duplicate Beige Book source URLs found.")

    years = pd.to_datetime(df["date"]).dt.year
    if years.min() < START_YEAR:
        raise ValueError(f"Historical Beige Book starts before allowed range: {years.min()} < {START_YEAR}")

    if years.max() > END_YEAR:
        raise ValueError(f"Historical Beige Book includes modern rows: {years.max()} > {END_YEAR}")

    short_docs = df[df["text_chars"] < MIN_TEXT_CHARS]
    if not short_docs.empty:
        raise ValueError(f"Short historical Beige Book text rows found: {len(short_docs)}")

    bad_hash = df[(df["text"].astype(str).str.len() > 0) & (df["text_sha256"].astype(str).str.len() != 64)]
    if not bad_hash.empty:
        raise ValueError(f"Bad SHA256 rows found: {len(bad_hash)}")

    print("[GeoScen:BeigeBook:Historical] validation passed")
    print(f"[GeoScen:BeigeBook:Historical] rows={len(df)}")
    print(f"[GeoScen:BeigeBook:Historical] date_range={df['date'].min()} to {df['date'].max()}")
    print("[GeoScen:BeigeBook:Historical] rows_by_year:")
    print(years.value_counts().sort_index().to_string())
    print("[GeoScen:BeigeBook:Historical] text_chars_summary:")
    print(df["text_chars"].describe().to_string())


if __name__ == "__main__":
    main()

    
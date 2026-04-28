"""
Validate combined Beige Book canonical manifest.

Run:
python -m spine.jobs.geoscen.beige_book.validate_beige_book_combined_canonical_v1
"""

from __future__ import annotations

import pandas as pd


OUTPUT_PATH = "data/geoscen/beige_book/beige_book_combined_canonical_v1.parquet"

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
    "source_layer",
]

MIN_ROWS = 225
MIN_TEXT_CHARS = 5_000


def main() -> None:
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few combined Beige Book rows: {len(df)} < {MIN_ROWS}")

    if df["document_id"].duplicated().any():
        raise ValueError("Duplicate document_id values found.")

    if df["url"].duplicated().any():
        raise ValueError("Duplicate Beige Book source URLs found.")

    bad_layers = sorted(set(df["source_layer"]) - {"historical", "modern"})
    if bad_layers:
        raise ValueError(f"Unexpected source_layer values: {bad_layers}")

    short_docs = df[df["text_chars"] < MIN_TEXT_CHARS]
    if not short_docs.empty:
        raise ValueError(f"Short combined Beige Book text rows found: {len(short_docs)}")

    years = pd.to_datetime(df["date"]).dt.year

    if years.min() > 1996:
        raise ValueError(f"Coverage starts too late: {years.min()} > 1996")

    if years.max() < 2025:
        raise ValueError(f"Coverage ends too early: {years.max()} < 2025")

    print("[GeoScen:BeigeBook:Combined] validation passed")
    print(f"[GeoScen:BeigeBook:Combined] rows={len(df)}")
    print(f"[GeoScen:BeigeBook:Combined] date_range={df['date'].min()} to {df['date'].max()}")
    print("[GeoScen:BeigeBook:Combined] rows_by_layer:")
    print(df["source_layer"].value_counts().to_string())
    print("[GeoScen:BeigeBook:Combined] rows_by_year:")
    print(years.value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()


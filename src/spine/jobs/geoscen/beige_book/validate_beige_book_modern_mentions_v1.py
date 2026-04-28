"""
Validate Beige Book modern district mention dataset.

Run:
python -m spine.jobs.geoscen.beige_book.validate_beige_book_modern_mentions_v1
"""

from __future__ import annotations

import pandas as pd


OUTPUT_PATH = "data/geoscen/beige_book/beige_book_modern_mentions_v1.parquet"

REQUIRED_COLUMNS = [
    "mention_id",
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
    "sentence_index",
    "sentence",
    "sentence_chars",
    "sentence_sha256",
    "mentioned_at_utc",
    "version",
]

MIN_ROWS = 500
MIN_SENTENCE_CHARS = 20


def main() -> None:
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few modern district mentions: {len(df)} < {MIN_ROWS}")

    if df["mention_id"].duplicated().any():
        raise ValueError("Duplicate mention_id values found.")

    years = pd.to_datetime(df["date"]).dt.year

    if years.min() < 2017:
        raise ValueError(f"Modern mentions include pre-2017 rows: {years.min()}")

    if years.max() < 2025:
        raise ValueError(f"Modern mentions coverage ends too early: {years.max()}")

    short_rows = df[df["sentence_chars"] < MIN_SENTENCE_CHARS]
    if not short_rows.empty:
        raise ValueError(f"Short mention rows found: {len(short_rows)}")

    district_counts = df["district_name"].value_counts()
    if len(district_counts) != 12:
        raise ValueError(f"Expected 12 districts, found {len(district_counts)}")

    bad_hash = df[
        (df["sentence"].astype(str).str.len() > 0)
        & (df["sentence_sha256"].astype(str).str.len() != 64)
    ]
    if not bad_hash.empty:
        raise ValueError(f"Bad SHA256 rows found: {len(bad_hash)}")

    print("[GeoScen:BeigeBook:ModernMentions] validation passed")
    print(f"[GeoScen:BeigeBook:ModernMentions] rows={len(df)}")
    print(f"[GeoScen:BeigeBook:ModernMentions] date_range={df['date'].min()} to {df['date'].max()}")
    print("[GeoScen:BeigeBook:ModernMentions] rows_by_district:")
    print(df["district_name"].value_counts().sort_index().to_string())
    print("[GeoScen:BeigeBook:ModernMentions] rows_by_region:")
    print(df["region"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()


"""
Validate Beige Book district-level segments.

Run:
python -m spine.jobs.geoscen.beige_book.validate_beige_book_district_segments_v1
"""

from __future__ import annotations

import pandas as pd


OUTPUT_PATH = "data/geoscen/beige_book/beige_book_district_segments_v1.parquet"

REQUIRED_COLUMNS = [
    "segment_id",
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
    "section_type",
    "section_index",
    "section_title",
    "text",
    "text_chars",
    "text_sha256",
    "segmented_at_utc",
    "version",
]

MIN_ROWS = 700
MIN_TEXT_CHARS = 500


def main() -> None:
    df = pd.read_parquet(OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few district segment rows: {len(df)} < {MIN_ROWS}")

    if df["segment_id"].duplicated().any():
        raise ValueError("Duplicate segment_id values found.")

    years = pd.to_datetime(df["date"]).dt.year

    if years.max() < 2016:
        raise ValueError("Segmentation missing expected historical coverage (~2016 cutoff).")

    short_docs = df[df["text_chars"] < MIN_TEXT_CHARS]
    if not short_docs.empty:
        raise ValueError(f"Short district segments found: {len(short_docs)}")

    district_counts = df["district_name"].value_counts()
    if len(district_counts) != 12:
        raise ValueError(f"Expected 12 districts, found {len(district_counts)}")

    bad_hash = df[(df["text"].astype(str).str.len() > 0) & (df["text_sha256"].astype(str).str.len() != 64)]
    if not bad_hash.empty:
        raise ValueError(f"Bad SHA256 rows found: {len(bad_hash)}")

    print("[GeoScen:BeigeBook:Districts] validation passed")
    print(f"[GeoScen:BeigeBook:Districts] rows={len(df)}")
    print(f"[GeoScen:BeigeBook:Districts] date_range={df['date'].min()} to {df['date'].max()}")
    print("[GeoScen:BeigeBook:Districts] rows_by_district:")
    print(df["district_name"].value_counts().sort_index().to_string())
    print("[GeoScen:BeigeBook:Districts] rows_by_region:")
    print(df["region"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()


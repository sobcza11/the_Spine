"""
Validate FOMC historical PDF text v2 parquet.

Run:
python -m spine.jobs.geoscen.fomc.historical.validate_fomc_historical_pdf_text_v2
"""

from __future__ import annotations

import pandas as pd


TEXT_OUTPUT_PATH = "data/geoscen/fomc/fomc_historical_pdf_text_v2.parquet"

REQUIRED_COLUMNS = [
    "document_id",
    "date",
    "year",
    "source",
    "document_family",
    "document_type",
    "title",
    "url",
    "file_format",
    "text",
    "text_sha256",
    "text_chars",
    "extract_status",
    "extract_error",
    "extracted_at_utc",
    "version",
]

TARGET_DOCUMENT_TYPES = {
    "minutes",
    "minutes_of_actions",
    "record_of_policy_actions",
    "transcript",
    "conference_call",
}

MIN_ROWS = 400
MIN_TEXT_CHARS = 500


def main() -> None:
    df = pd.read_parquet(TEXT_OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few extracted rows: {len(df)} < {MIN_ROWS}")

    if df["document_id"].duplicated().any():
        raise ValueError("Duplicate document_id values found.")

    bad_types = sorted(set(df["document_type"]) - TARGET_DOCUMENT_TYPES)
    if bad_types:
        raise ValueError(f"Unexpected document_type values: {bad_types}")

    bad_status = df[df["extract_status"] != "ok"]
    if not bad_status.empty:
        raise ValueError(f"Non-ok extraction rows found: {len(bad_status)}")

    bad_format = df[df["file_format"] != "pdf"]
    if not bad_format.empty:
        raise ValueError(f"Non-PDF rows found: {len(bad_format)}")

    short_docs = df[df["text_chars"] < MIN_TEXT_CHARS]
    if not short_docs.empty:
        raise ValueError(f"Short text rows found: {len(short_docs)}")

    bad_hash = df[(df["text"].astype(str).str.len() > 0) & (df["text_sha256"].astype(str).str.len() != 64)]
    if not bad_hash.empty:
        raise ValueError(f"Bad SHA256 rows found: {len(bad_hash)}")

    print("[GeoScen:FOMC:PDF:v2] validation passed")
    print(f"[GeoScen:FOMC:PDF:v2] rows={len(df)}")
    print(f"[GeoScen:FOMC:PDF:v2] date_range={df['date'].min()} to {df['date'].max()}")
    print("[GeoScen:FOMC:PDF:v2] document_type_counts:")
    print(df["document_type"].value_counts().to_string())
    print("[GeoScen:FOMC:PDF:v2] text_chars_summary:")
    print(df.groupby("document_type")["text_chars"].describe().to_string())


if __name__ == "__main__":
    main()


"""
Validate FOMC Minutes canonical parquet — v1.

Run:
python -m spine.jobs.geoscen.validate_fomc_minutes_t1
"""

from __future__ import annotations

import pandas as pd

from spine.jobs.geoscen.fomc.fomc_constants import LOCAL_OUTPUT_PATH, REQUIRED_COLUMNS


MIN_ROWS = 50
MIN_TEXT_CHARS = 2_000


def main() -> None:
    df = pd.read_parquet(LOCAL_OUTPUT_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Too few FOMC minutes rows: {len(df)} < {MIN_ROWS}")

    if df["document_id"].duplicated().any():
        raise ValueError("Duplicate document_id values found.")

    if df["url"].duplicated().any():
        raise ValueError("Duplicate FOMC source URLs found.")

    null_counts = df[REQUIRED_COLUMNS].isna().sum()
    bad_nulls = null_counts[null_counts > 0]
    if not bad_nulls.empty:
        raise ValueError(f"Nulls found in required columns: {bad_nulls.to_dict()}")

    short_docs = df[df["text"].astype(str).str.len() < MIN_TEXT_CHARS]
    if not short_docs.empty:
        raise ValueError(f"Short document text rows found: {len(short_docs)}")

    print("[GeoScen:FOMC] validation passed")
    print(f"[GeoScen:FOMC] rows={len(df)}")
    print(f"[GeoScen:FOMC] date_range={df['date'].min()} to {df['date'].max()}")


if __name__ == "__main__":
    main()

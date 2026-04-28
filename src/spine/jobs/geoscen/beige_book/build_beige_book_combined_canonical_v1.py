"""
Build combined Beige Book canonical manifest.

Inputs:
- Modern Beige Book: data/geoscen/beige_book/beige_book_canonical_v1.parquet
- Historical Beige Book: data/geoscen/beige_book/beige_book_historical_canonical_t1.parquet

Output:
- data/geoscen/beige_book/beige_book_combined_canonical_v1.parquet

Run:
python -m spine.jobs.geoscen.beige_book.build_beige_book_combined_canonical_v1
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


MODERN_PATH = "data/geoscen/beige_book/beige_book_canonical_v1.parquet"
HISTORICAL_PATH = "data/geoscen/beige_book/beige_book_historical_canonical_t1.parquet"
OUTPUT_PATH = "data/geoscen/beige_book/beige_book_combined_canonical_v1.parquet"


def main() -> None:
    modern = pd.read_parquet(MODERN_PATH).copy()
    historical = pd.read_parquet(HISTORICAL_PATH).copy()

    modern["source_layer"] = "modern"
    historical["source_layer"] = "historical"

    df = pd.concat([historical, modern], ignore_index=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()

    df = df.sort_values(["date", "source_layer", "url"])
    df = df.drop_duplicates(["date", "url"]).reset_index(drop=True)

    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print("[GeoScen:BeigeBook:Combined] build complete")
    print(f"[GeoScen:BeigeBook:Combined] rows={len(df)}")
    print(f"[GeoScen:BeigeBook:Combined] min_date={df['date'].min().date()}")
    print(f"[GeoScen:BeigeBook:Combined] max_date={df['date'].max().date()}")
    print("[GeoScen:BeigeBook:Combined] rows_by_layer:")
    print(df["source_layer"].value_counts().to_string())
    print("[GeoScen:BeigeBook:Combined] rows_by_year:")
    print(df["date"].dt.year.value_counts().sort_index().to_string())
    print(f"[GeoScen:BeigeBook:Combined] wrote={OUTPUT_PATH}")


if __name__ == "__main__":
    main()


"""
Build Beige Book district-level text segments.

Input:
data/geoscen/beige_book/beige_book_combined_canonical_v1.parquet

Output:
data/geoscen/beige_book/beige_book_district_segments_v1.parquet

Run:
python -m spine.jobs.geoscen.beige_book.build_beige_book_district_segments_v1
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


INPUT_PATH = "data/geoscen/beige_book/beige_book_combined_canonical_v1.parquet"
OUTPUT_PATH = "data/geoscen/beige_book/beige_book_district_segments_v1.parquet"

VERSION = "beige_book_district_segments_v1"

DISTRICTS = [
    ("Boston", "BOS", 1),
    ("New York", "NY", 2),
    ("Philadelphia", "PHI", 3),
    ("Cleveland", "CLE", 4),
    ("Richmond", "RIC", 5),
    ("Atlanta", "ATL", 6),
    ("Chicago", "CHI", 7),
    ("St. Louis", "STL", 8),
    ("Minneapolis", "MIN", 9),
    ("Kansas City", "KC", 10),
    ("Dallas", "DAL", 11),
    ("San Francisco", "SF", 12),
]

DISTRICT_TO_REGION = {
    "Boston": ("Northeast", "NE", 1),
    "New York": ("Northeast", "NE", 1),
    "Philadelphia": ("Northeast", "NE", 1),
    "Cleveland": ("Midwest", "MW", 2),
    "Chicago": ("Midwest", "MW", 2),
    "St. Louis": ("Midwest", "MW", 2),
    "Richmond": ("Southeast", "SE", 3),
    "Atlanta": ("Southeast", "SE", 3),
    "Dallas": ("Southeast", "SE", 3),
    "Minneapolis": ("Plains", "PL", 4),
    "Kansas City": ("Plains", "PL", 4),
    "San Francisco": ("West", "W", 5),
}


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _district_body(text: str) -> str:
    match = re.search(r"\bFull report\b", text, flags=re.IGNORECASE)
    if match:
        return text[match.end():]
    return text


def _find_district_positions(text: str) -> list[tuple[str, str, int, int]]:
    positions = []

    for district_name, district_code, district_order in DISTRICTS:
        pattern = rf"\b{re.escape(district_name)}\s+(?:District|reports?|indicated|noted)\b"

        matches = list(re.finditer(pattern, text, flags=re.IGNORECASE))

        if matches:
            # Use FIRST occurrence AFTER full report
            positions.append((district_name, district_code, district_order, matches[0].start()))

    return sorted(positions, key=lambda x: x[3])


def _segment_document(row: pd.Series, segmented_at_utc: str) -> list[dict[str, object]]:
    raw_text = _clean_text(str(row["text"]))
    text = _district_body(raw_text)
    positions = _find_district_positions(text)

    segments = []

    if len(positions) < 10:
        return segments

    for idx, (district_name, district_code, district_order, start_pos) in enumerate(positions):
        end_pos = positions[idx + 1][3] if idx + 1 < len(positions) else len(text)
        segment_text = _clean_text(text[start_pos:end_pos])

        if len(segment_text) < 500:
            continue

        region, region_code, region_order = DISTRICT_TO_REGION[district_name]

        stable_key = f"{row['document_id']}|{district_code}|{_sha256(segment_text)[:12]}"
        segment_id = f"bbseg_{row['date'].strftime('%Y%m%d')}_{district_code}_{_sha256(stable_key)[:12]}"

        segments.append(
            {
                "segment_id": segment_id,
                "document_id": row["document_id"],
                "date": row["date"],
                "year": int(row["year"]),
                "source": row["source"],
                "document_family": row["document_family"],
                "source_layer": row["source_layer"],
                "url": row["url"],
                "district_name": district_name,
                "district_code": district_code,
                "district_order": district_order,
                "region": region,
                "region_code": region_code,
                "region_order": region_order,
                "section_type": "district_section",
                "section_index": district_order,
                "section_title": district_name,
                "text": segment_text,
                "text_chars": len(segment_text),
                "text_sha256": _sha256(segment_text),
                "segmented_at_utc": segmented_at_utc,
                "version": VERSION,
            }
        )

    return segments


def main() -> None:
    segmented_at_utc = datetime.now(timezone.utc).isoformat()

    df = pd.read_parquet(INPUT_PATH)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()

    rows = []

    for _, row in df.iterrows():
        rows.extend(_segment_document(row, segmented_at_utc))

    if not rows:
        raise RuntimeError("No Beige Book district segments created.")

    out = pd.DataFrame(rows)
    out = out.sort_values(["date", "district_order"]).drop_duplicates("segment_id").reset_index(drop=True)

    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)

    print("[GeoScen:BeigeBook:Districts] build complete")
    print(f"[GeoScen:BeigeBook:Districts] rows={len(out)}")
    print(f"[GeoScen:BeigeBook:Districts] min_date={out['date'].min().date()}")
    print(f"[GeoScen:BeigeBook:Districts] max_date={out['date'].max().date()}")
    print("[GeoScen:BeigeBook:Districts] rows_by_district:")
    print(out["district_name"].value_counts().sort_index().to_string())
    print("[GeoScen:BeigeBook:Districts] rows_by_region:")
    print(out["region"].value_counts().sort_index().to_string())
    print(f"[GeoScen:BeigeBook:Districts] wrote={OUTPUT_PATH}")


if __name__ == "__main__":
    main()


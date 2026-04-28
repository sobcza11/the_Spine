"""
Build Beige Book modern district mention dataset.

Scope:
2017-present modern Beige Book narrative pages.

Run:
python -m spine.jobs.geoscen.beige_book.build_beige_book_modern_mentions_v1
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


INPUT_PATH = "data/geoscen/beige_book/beige_book_combined_canonical_v1.parquet"
OUTPUT_PATH = "data/geoscen/beige_book/beige_book_modern_mentions_v1.parquet"

VERSION = "beige_book_modern_mentions_v1"

DISTRICTS = [
    ("Boston", "BOS", 1, "Northeast", "NE", 1),
    ("New York", "NY", 2, "Northeast", "NE", 1),
    ("Philadelphia", "PHI", 3, "Northeast", "NE", 1),
    ("Cleveland", "CLE", 4, "Midwest", "MW", 2),
    ("Richmond", "RIC", 5, "Southeast", "SE", 3),
    ("Atlanta", "ATL", 6, "Southeast", "SE", 3),
    ("Chicago", "CHI", 7, "Midwest", "MW", 2),
    ("St. Louis", "STL", 8, "Midwest", "MW", 2),
    ("Minneapolis", "MIN", 9, "Plains", "PL", 4),
    ("Kansas City", "KC", 10, "Plains", "PL", 4),
    ("Dallas", "DAL", 11, "Southeast", "SE", 3),
    ("San Francisco", "SF", 12, "West", "W", 5),
]


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _body_after_full_report(text: str) -> str:
    match = re.search(r"\bFull report\b", text, flags=re.IGNORECASE)
    if match:
        return text[match.end():]
    return text


def _split_sentences(text: str) -> list[str]:
    text = _clean_text(text)
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]


def main() -> None:
    mentioned_at_utc = datetime.now(timezone.utc).isoformat()

    df = pd.read_parquet(INPUT_PATH)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].dt.year >= 2017].copy()

    rows = []

    for _, row in df.iterrows():
        body = _body_after_full_report(str(row["text"]))
        sentences = _split_sentences(body)

        for sentence_index, sentence in enumerate(sentences):
            if len(sentence) < 20:
                continue

            for district_name, district_code, district_order, region, region_code, region_order in DISTRICTS:
                if district_name == "St. Louis":
                    match = re.search(r"St\.?\s+Louis", sentence, flags=re.IGNORECASE)
                else:
                    match = re.search(rf"\b{re.escape(district_name)}\b", sentence, flags=re.IGNORECASE)

                if not match:
                    continue
                
                stable_key = f"{row['document_id']}|{sentence_index}|{district_code}|{sentence}"
                mention_id = f"bbmention_{row['date'].strftime('%Y%m%d')}_{district_code}_{_sha256(stable_key)[:12]}"

                rows.append(
                    {
                        "mention_id": mention_id,
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
                        "sentence_index": sentence_index,
                        "sentence": sentence,
                        "sentence_chars": len(sentence),
                        "sentence_sha256": _sha256(sentence),
                        "mentioned_at_utc": mentioned_at_utc,
                        "version": VERSION,
                    }
                )

    if not rows:
        raise RuntimeError("No modern Beige Book district mentions created.")

    out = pd.DataFrame(rows)
    out = out.sort_values(["date", "district_order", "sentence_index"]).drop_duplicates("mention_id").reset_index(drop=True)

    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)

    print("[GeoScen:BeigeBook:ModernMentions] build complete")
    print(f"[GeoScen:BeigeBook:ModernMentions] rows={len(out)}")
    print(f"[GeoScen:BeigeBook:ModernMentions] min_date={out['date'].min().date()}")
    print(f"[GeoScen:BeigeBook:ModernMentions] max_date={out['date'].max().date()}")
    print("[GeoScen:BeigeBook:ModernMentions] rows_by_district:")
    print(out["district_name"].value_counts().sort_index().to_string())
    print("[GeoScen:BeigeBook:ModernMentions] rows_by_region:")
    print(out["region"].value_counts().sort_index().to_string())
    print(f"[GeoScen:BeigeBook:ModernMentions] wrote={OUTPUT_PATH}")


if __name__ == "__main__":
    main()


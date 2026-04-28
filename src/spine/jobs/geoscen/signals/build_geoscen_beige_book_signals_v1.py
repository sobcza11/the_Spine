"""
Build GeoScen v1 Beige Book signals.

Inputs:
- data/geoscen/beige_book/beige_book_district_segments_v1.parquet
- data/geoscen/beige_book/beige_book_modern_mentions_v1.parquet

Output:
- data/geoscen/signals/geoscen_beige_book_signals_v1.parquet

Run:
python -m spine.jobs.geoscen.signals.build_geoscen_beige_book_signals_v1
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


HISTORICAL_SEGMENTS_PATH = "data/geoscen/beige_book/beige_book_district_segments_v1.parquet"
MODERN_MENTIONS_PATH = "data/geoscen/beige_book/beige_book_modern_mentions_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/geoscen_beige_book_signals_v1.parquet"

VERSION = "geoscen_beige_book_signals_v1"


LABOR_POSITIVE = [
    "labor shortages",
    "shortage of workers",
    "difficulty finding workers",
    "difficulty hiring",
    "tight labor",
    "wage pressures",
    "wages increased",
    "wages rose",
    "compensation increased",
    "hiring remained strong",
]

LABOR_NEGATIVE = [
    "layoffs",
    "reduced headcounts",
    "hiring slowed",
    "hiring freeze",
    "soft labor",
    "labor market softened",
    "reduced staffing",
    "weaker employment",
    "job cuts",
]

INFLATION_POSITIVE = [
    "prices increased",
    "prices rose",
    "price pressures",
    "input costs",
    "cost pressures",
    "higher costs",
    "inflationary pressures",
    "passed along",
    "raised prices",
    "energy prices increased",
]

INFLATION_NEGATIVE = [
    "prices declined",
    "prices fell",
    "price pressures eased",
    "cost pressures eased",
    "discounting",
    "lower prices",
    "stable prices",
    "little changed prices",
]

CREDIT_POSITIVE_STRESS = [
    "tight credit",
    "tighter credit",
    "tight lending standards",
    "lending standards tightened",
    "loan demand weakened",
    "credit quality deteriorated",
    "delinquencies increased",
    "delinquencies rose",
    "credit conditions tightened",
    "financing constraints",
]

CREDIT_NEGATIVE_STRESS = [
    "credit quality improved",
    "loan demand increased",
    "lending increased",
    "credit conditions improved",
    "standards eased",
    "delinquencies declined",
    "stable credit conditions",
]


def _count_terms(text: str, terms: list[str]) -> int:
    text_l = str(text).lower()
    total = 0
    for term in terms:
        total += len(re.findall(rf"\b{re.escape(term.lower())}\b", text_l))
    return total


def _bounded_score(pos: int, neg: int) -> float:
    denom = pos + neg
    if denom == 0:
        return 0.0
    return round((pos - neg) / denom, 6)


def _score_text(text: str) -> dict[str, float | int]:
    labor_pos = _count_terms(text, LABOR_POSITIVE)
    labor_neg = _count_terms(text, LABOR_NEGATIVE)

    inflation_pos = _count_terms(text, INFLATION_POSITIVE)
    inflation_neg = _count_terms(text, INFLATION_NEGATIVE)

    credit_pos = _count_terms(text, CREDIT_POSITIVE_STRESS)
    credit_neg = _count_terms(text, CREDIT_NEGATIVE_STRESS)

    return {
        "labor_pos_hits": labor_pos,
        "labor_neg_hits": labor_neg,
        "inflation_pos_hits": inflation_pos,
        "inflation_neg_hits": inflation_neg,
        "credit_stress_pos_hits": credit_pos,
        "credit_stress_neg_hits": credit_neg,
        "labor_pressure_score": _bounded_score(labor_pos, labor_neg),
        "inflation_tone_score": _bounded_score(inflation_pos, inflation_neg),
        "credit_stress_score": _bounded_score(credit_pos, credit_neg),
    }


def _load_historical() -> pd.DataFrame:
    df = pd.read_parquet(HISTORICAL_SEGMENTS_PATH).copy()
    df["text_unit"] = df["text"]
    df["signal_layer"] = "historical_district_segment"
    return df[
        [
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
            "text_unit",
            "signal_layer",
        ]
    ]


def _load_modern() -> pd.DataFrame:
    df = pd.read_parquet(MODERN_MENTIONS_PATH).copy()
    df["text_unit"] = df["sentence"]
    df["signal_layer"] = "modern_district_mention"
    return df[
        [
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
            "text_unit",
            "signal_layer",
        ]
    ]


def main() -> None:
    built_at_utc = datetime.now(timezone.utc).isoformat()

    historical = _load_historical()
    modern = _load_modern()

    df = pd.concat([historical, modern], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()

    score_rows = []
    for _, row in df.iterrows():
        scores = _score_text(str(row["text_unit"]))
        score_rows.append(scores)

    scores_df = pd.DataFrame(score_rows)
    out = pd.concat([df.reset_index(drop=True), scores_df], axis=1)

    hit_cols = [
        "labor_pos_hits",
        "labor_neg_hits",
        "inflation_pos_hits",
        "inflation_neg_hits",
        "credit_stress_pos_hits",
        "credit_stress_neg_hits",
    ]

    out["total_signal_hits"] = out[hit_cols].sum(axis=1)
    out["built_at_utc"] = built_at_utc
    out["version"] = VERSION

    out = out.sort_values(["date", "district_order", "signal_layer"]).reset_index(drop=True)

    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)

    print("[GeoScen:Signals:BeigeBook] build complete")
    print(f"[GeoScen:Signals:BeigeBook] rows={len(out)}")
    print(f"[GeoScen:Signals:BeigeBook] min_date={out['date'].min().date()}")
    print(f"[GeoScen:Signals:BeigeBook] max_date={out['date'].max().date()}")
    print("[GeoScen:Signals:BeigeBook] rows_by_signal_layer:")
    print(out["signal_layer"].value_counts().to_string())
    print("[GeoScen:Signals:BeigeBook] hit_summary:")
    print(out[hit_cols + ["total_signal_hits"]].sum().to_string())
    print(f"[GeoScen:Signals:BeigeBook] wrote={OUTPUT_PATH}")


if __name__ == "__main__":
    main()


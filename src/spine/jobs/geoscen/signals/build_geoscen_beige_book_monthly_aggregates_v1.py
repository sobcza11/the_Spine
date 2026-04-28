"""
Build monthly GeoScen Beige Book aggregate signals.

Input:
data/geoscen/signals/geoscen_beige_book_signals_v1.parquet

Output:
data/geoscen/signals/geoscen_beige_book_monthly_aggregates_v1.parquet

Run:
python -m spine.jobs.geoscen.signals.build_geoscen_beige_book_monthly_aggregates_v1
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


INPUT_PATH = "data/geoscen/signals/geoscen_beige_book_signals_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/geoscen_beige_book_monthly_aggregates_v1.parquet"

VERSION = "geoscen_beige_book_monthly_aggregates_v1"

SCORE_COLS = [
    "labor_pressure_score",
    "inflation_tone_score",
    "credit_stress_score",
]

HIT_COLS = [
    "labor_pos_hits",
    "labor_neg_hits",
    "inflation_pos_hits",
    "inflation_neg_hits",
    "credit_stress_pos_hits",
    "credit_stress_neg_hits",
    "total_signal_hits",
]


def _aggregate(df: pd.DataFrame, group_cols: list[str], level: str) -> pd.DataFrame:
    agg = (
        df.groupby(group_cols, dropna=False)
        .agg(
            text_units=("text_unit", "count"),
            documents=("document_id", "nunique"),
            labor_pressure_score=("labor_pressure_score", "mean"),
            inflation_tone_score=("inflation_tone_score", "mean"),
            credit_stress_score=("credit_stress_score", "mean"),
            labor_pos_hits=("labor_pos_hits", "sum"),
            labor_neg_hits=("labor_neg_hits", "sum"),
            inflation_pos_hits=("inflation_pos_hits", "sum"),
            inflation_neg_hits=("inflation_neg_hits", "sum"),
            credit_stress_pos_hits=("credit_stress_pos_hits", "sum"),
            credit_stress_neg_hits=("credit_stress_neg_hits", "sum"),
            total_signal_hits=("total_signal_hits", "sum"),
        )
        .reset_index()
    )

    agg["aggregation_level"] = level
    return agg


def main() -> None:
    built_at_utc = datetime.now(timezone.utc).isoformat()

    df = pd.read_parquet(INPUT_PATH)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()

    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

    national = _aggregate(
        df,
        ["month"],
        "national",
    )

    national["district_name"] = "ALL"
    national["district_code"] = "ALL"
    national["district_order"] = 0
    national["region"] = "ALL"
    national["region_code"] = "ALL"
    national["region_order"] = 0

    regional = _aggregate(
        df,
        ["month", "region", "region_code", "region_order"],
        "region",
    )

    regional["district_name"] = "ALL"
    regional["district_code"] = "ALL"
    regional["district_order"] = 0

    district = _aggregate(
        df,
        [
            "month",
            "region",
            "region_code",
            "region_order",
            "district_name",
            "district_code",
            "district_order",
        ],
        "district",
    )

    out = pd.concat([national, regional, district], ignore_index=True)

    out["labor_net_hits"] = out["labor_pos_hits"] - out["labor_neg_hits"]
    out["inflation_net_hits"] = out["inflation_pos_hits"] - out["inflation_neg_hits"]
    out["credit_stress_net_hits"] = out["credit_stress_pos_hits"] - out["credit_stress_neg_hits"]

    out["built_at_utc"] = built_at_utc
    out["version"] = VERSION

    col_order = [
        "month",
        "aggregation_level",
        "region",
        "region_code",
        "region_order",
        "district_name",
        "district_code",
        "district_order",
        "text_units",
        "documents",
        "labor_pressure_score",
        "inflation_tone_score",
        "credit_stress_score",
        "labor_pos_hits",
        "labor_neg_hits",
        "labor_net_hits",
        "inflation_pos_hits",
        "inflation_neg_hits",
        "inflation_net_hits",
        "credit_stress_pos_hits",
        "credit_stress_neg_hits",
        "credit_stress_net_hits",
        "total_signal_hits",
        "built_at_utc",
        "version",
    ]

    out = out[col_order].sort_values(
        ["month", "aggregation_level", "region_order", "district_order"]
    ).reset_index(drop=True)

    output_path = Path(OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)

    print("[GeoScen:Signals:MonthlyAggregates] build complete")
    print(f"[GeoScen:Signals:MonthlyAggregates] rows={len(out)}")
    print(f"[GeoScen:Signals:MonthlyAggregates] min_month={out['month'].min().date()}")
    print(f"[GeoScen:Signals:MonthlyAggregates] max_month={out['month'].max().date()}")
    print("[GeoScen:Signals:MonthlyAggregates] rows_by_level:")
    print(out["aggregation_level"].value_counts().to_string())
    print("[GeoScen:Signals:MonthlyAggregates] total_hits_by_level:")
    print(out.groupby("aggregation_level")["total_signal_hits"].sum().to_string())
    print(f"[GeoScen:Signals:MonthlyAggregates] wrote={OUTPUT_PATH}")


if __name__ == "__main__":
    main()


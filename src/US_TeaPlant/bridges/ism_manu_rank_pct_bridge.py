from __future__ import annotations

"""
US_TeaPlant.bridges.ism_manu_rank_pct_bridge

Builds the canonical US ISM Manufacturing PMI-rank-percent-by-industry leaf
from ism_pmi_transp.xlsx (sheet: manu_rank_pct).

R2 key: spine_us/us_ism_manu_pmi_rank_pct_by_industry_canonical.parquet
"""

import os
import pandas as pd

from common.r2_client import write_parquet_to_r2

R2_ISM_MANU_PMI_RANK_PCT_KEY = (
    "spine_us/us_ism_manu_pmi_rank_pct_by_industry_canonical.parquet"
)


def _load_manu_rank_pct_excel(
    excel_path: str,
    sheet_name: str = "manu_rank_pct",
) -> pd.DataFrame:
    if not os.path.exists(excel_path):
        raise FileNotFoundError(
            f"ISM Excel file not found at {excel_path}. "
            "Update the path or place the file there."
        )

    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    if df.empty:
        raise RuntimeError(f"Sheet '{sheet_name}' in {excel_path} is empty.")

    date_col = df.columns[0]
    df = df.rename(columns={date_col: "as_of_date"})
    df = df[df["as_of_date"].notna()].copy()
    return df


def _reshape_manu_rank_pct_to_canonical(df_raw: pd.DataFrame) -> pd.DataFrame:
    sector_cols = [c for c in df_raw.columns if c != "as_of_date"]

    df_long = df_raw.melt(
        id_vars=["as_of_date"],
        value_vars=sector_cols,
        var_name="sector_name",
        value_name="pmi_rank_pct",
    )

    df_long = df_long[df_long["pmi_rank_pct"].notna()].copy()

    df_long["as_of_date"] = pd.to_datetime(df_long["as_of_date"])
    df_long["as_of_date"] = df_long["as_of_date"].values.astype("datetime64[M]")

    df_long["country"] = "US"
    df_long["ccy"] = "USD"
    df_long["leaf_group"] = "MACRO_ISM"
    df_long["leaf_name"] = "us_ism_manu_pmi_rank_pct_by_industry_canonical"
    df_long["source_system"] = "ISM_MANU_EXCEL"
    df_long["updated_at"] = pd.Timestamp.utcnow()

    df_long = df_long[
        [
            "as_of_date",
            "country",
            "ccy",
            "sector_name",
            "pmi_rank_pct",
            "leaf_group",
            "leaf_name",
            "source_system",
            "updated_at",
        ]
    ].sort_values(["as_of_date", "sector_name"])

    return df_long.reset_index(drop=True)


def build_us_ism_manu_pmi_rank_pct_by_industry_canonical(
    excel_path: str,
    sheet_name: str = "manu_rank_pct",
) -> pd.DataFrame:
    print(
        f"[ISM-MANU-RANK-PCT] Loading manu_rank_pct from {excel_path} (sheet={sheet_name}) …"
    )
    df_raw = _load_manu_rank_pct_excel(
        excel_path=excel_path,
        sheet_name=sheet_name,
    )

    print(
        f"[ISM-MANU-RANK-PCT] Reshaping manu_rank_pct to canonical form "
        f"(rows={len(df_raw)}) …"
    )
    df_canonical = _reshape_manu_rank_pct_to_canonical(df_raw)

    write_parquet_to_r2(df_canonical, R2_ISM_MANU_PMI_RANK_PCT_KEY, index=False)

    print(
        f"[ISM-MANU-RANK-PCT] Wrote canonical US ISM Manufacturing PMI-rank-pct-by-industry leaf "
        f"to R2 at {R2_ISM_MANU_PMI_RANK_PCT_KEY} (rows={len(df_canonical)})"
    )

    return df_canonical


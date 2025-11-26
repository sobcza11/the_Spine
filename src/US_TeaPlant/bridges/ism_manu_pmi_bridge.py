from __future__ import annotations

"""
US_TeaPlant.bridges.ism_manu_pmi_bridge

Builds the canonical US ISM Manufacturing PMI-by-industry leaf for the_Spine
from the local Excel workbook ism_pmi_transp.xlsx (sheet: manu_pmi).

Schema written to R2 at spine_us/us_ism_manu_pmi_by_industry_canonical.parquet:

    as_of_date      datetime64[ns]
    country         string  ("US")
    ccy             string  ("USD")
    sector_name     string
    pmi_value       float64
    leaf_group      string  ("MACRO_ISM")
    leaf_name       string  ("us_ism_manu_pmi_by_industry_canonical")
    source_system   string  ("ISM_MANU_EXCEL")
    updated_at      datetime64[ns]
"""

import os
import pandas as pd

from common.r2_client import write_parquet_to_r2

R2_ISM_MANU_PMI_KEY = "spine_us/us_ism_manu_pmi_by_industry_canonical.parquet"


def _load_manu_pmi_excel(
    excel_path: str,
    sheet_name: str = "manu_pmi",
) -> pd.DataFrame:
    if not os.path.exists(excel_path):
        raise FileNotFoundError(
            f"ISM Excel file not found at {excel_path}. "
            "Update the path or place the file there."
        )

    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    if df.empty:
        raise RuntimeError(f"Sheet '{sheet_name}' in {excel_path} is empty.")

    # Assume first column is the date column
    date_col = df.columns[0]
    df = df.rename(columns={date_col: "as_of_date"})

    # Drop rows without a date
    df = df[df["as_of_date"].notna()].copy()

    return df


def _reshape_manu_pmi_to_canonical(df_raw: pd.DataFrame) -> pd.DataFrame:
    # All columns except as_of_date are sectors
    sector_cols = [c for c in df_raw.columns if c != "as_of_date"]

    df_long = df_raw.melt(
        id_vars=["as_of_date"],
        value_vars=sector_cols,
        var_name="sector_name",
        value_name="pmi_value",
    )

    # Drop empty values
    df_long = df_long[df_long["pmi_value"].notna()].copy()

    # Ensure dates are first-of-month (Option A)
    df_long["as_of_date"] = pd.to_datetime(df_long["as_of_date"])
    df_long["as_of_date"] = df_long["as_of_date"].values.astype("datetime64[M]")

    # Static fields
    df_long["country"] = "US"
    df_long["ccy"] = "USD"
    df_long["leaf_group"] = "MACRO_ISM"
    df_long["leaf_name"] = "us_ism_manu_pmi_by_industry_canonical"
    df_long["source_system"] = "ISM_MANU_EXCEL"
    df_long["updated_at"] = pd.Timestamp.utcnow()

    df_long = df_long[
        [
            "as_of_date",
            "country",
            "ccy",
            "sector_name",
            "pmi_value",
            "leaf_group",
            "leaf_name",
            "source_system",
            "updated_at",
        ]
    ].sort_values(["as_of_date", "sector_name"])

    df_long = df_long.reset_index(drop=True)
    return df_long


def build_us_ism_manu_pmi_by_industry_canonical(
    excel_path: str,
    sheet_name: str = "manu_pmi",
) -> pd.DataFrame:
    print(
        f"[ISM-MANU-PMI] Loading manu_pmi from {excel_path} (sheet={sheet_name}) …"
    )
    df_raw = _load_manu_pmi_excel(excel_path=excel_path, sheet_name=sheet_name)

    print(
        f"[ISM-MANU-PMI] Reshaping manu_pmi to canonical form "
        f"(rows={len(df_raw)}) …"
    )
    df_canonical = _reshape_manu_pmi_to_canonical(df_raw)

    write_parquet_to_r2(df_canonical, R2_ISM_MANU_PMI_KEY, index=False)

    print(
        f"[ISM-MANU-PMI] Wrote canonical US ISM Manufacturing PMI-by-industry leaf "
        f"to R2 at {R2_ISM_MANU_PMI_KEY} (rows={len(df_canonical)})"
    )

    return df_canonical

from __future__ import annotations

import os
import pandas as pd

from common.r2_client import write_parquet_to_r2

R2_KEY = "spine_us/us_ism_nonmanu_pmi_by_industry_canonical.parquet"


def _load_excel(path: str, sheet: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"ISM Excel not found at {path}")
    df = pd.read_excel(path, sheet_name=sheet)
    if df.empty:
        raise RuntimeError(f"Sheet '{sheet}' empty in ISM Excel.")
    df = df.rename(columns={df.columns[0]: "as_of_date"})
    df = df[df["as_of_date"].notna()].copy()
    return df


def _reshape(df_raw: pd.DataFrame) -> pd.DataFrame:
    sector_cols = [c for c in df_raw.columns if c != "as_of_date"]

    df = df_raw.melt(
        id_vars=["as_of_date"],
        value_vars=sector_cols,
        var_name="sector_name",
        value_name="pmi_value",
    )

    df = df[df["pmi_value"].notna()].copy()

    # month-normalized date
    df["as_of_date"] = pd.to_datetime(df["as_of_date"]).values.astype("datetime64[M]")

    df["country"] = "US"
    df["ccy"] = "USD"
    df["leaf_group"] = "MACRO_ISM"
    df["leaf_name"] = "us_ism_nonmanu_pmi_by_industry_canonical"
    df["source_system"] = "ISM_NONMANU_EXCEL"
    df["updated_at"] = pd.Timestamp.utcnow()

    return df[
        [
            "as_of_date", "country", "ccy",
            "sector_name", "pmi_value",
            "leaf_group", "leaf_name", "source_system", "updated_at",
        ]
    ].sort_values(["as_of_date", "sector_name"]).reset_index(drop=True)


def build_us_ism_nonmanu_pmi_by_industry_canonical(
    excel_path: str,
    sheet_name: str = "serv_pmi",
) -> pd.DataFrame:
    print(f"[ISM-NONMANU-PMI] Loading {sheet_name} …")
    df_raw = _load_excel(excel_path, sheet_name)

    print("[ISM-NONMANU-PMI] Reshaping to canonical …")
    df_final = _reshape(df_raw)

    write_parquet_to_r2(df_final, R2_KEY, index=False)

    print(
        f"[ISM-NONMANU-PMI] Wrote canonical Services PMI-by-industry to R2 "
        f"(rows={len(df_final)})"
    )

    return df_final

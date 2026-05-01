import os
from pathlib import Path

import pandas as pd


REPO_ROOT = Path.cwd()

SOURCE_URL = "https://www.eia.gov/dnav/pet/hist_xls/WCESTUS1w.xls"

OUTPUT_PATH = (
    REPO_ROOT
    / "data"
    / "wti"
    / "us_wti_inventory_canonical.parquet"
)


def build_wti_inventory() -> pd.DataFrame:
    df = pd.read_excel(
        SOURCE_URL,
        sheet_name="Data 1",
        skiprows=2
    )

    df = df.rename(columns={
        df.columns[0]: "date",
        df.columns[1]: "inventory_thousand_barrels"
    })

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["inventory_thousand_barrels"] = pd.to_numeric(
        df["inventory_thousand_barrels"],
        errors="coerce"
    )

    df = (
        df
        .dropna(subset=["date", "inventory_thousand_barrels"])
        .sort_values("date")
        .reset_index(drop=True)
    )

    df["inventory_mmbbl"] = df["inventory_thousand_barrels"] / 1000.0
    df["value"] = df["inventory_mmbbl"]

    df = df[[
        "date",
        "inventory_mmbbl",
        "value",
        "inventory_thousand_barrels"
    ]]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"WTI INVENTORY CANONICAL BUILT: {OUTPUT_PATH}")
    print(f"ROWS: {len(df)}")
    print(f"LATEST DATE: {df['date'].max().strftime('%Y-%m-%d')}")
    print(f"LATEST INVENTORY MMBL: {df['inventory_mmbbl'].iloc[-1]:.3f}")

    return df


if __name__ == "__main__":
    build_wti_inventory()
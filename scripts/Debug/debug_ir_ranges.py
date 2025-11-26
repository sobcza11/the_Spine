from __future__ import annotations
import pandas as pd
from common.r2_client import read_parquet_from_r2

KEY = "spine_us/us_ir_yields_canonical.parquet"

def main():
    df = read_parquet_from_r2(KEY)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])

    print("\n=== IR YIELDS DATE RANGES ===\n")
    for ccy in sorted(df["ccy"].unique()):
        df_ccy = df[df["ccy"] == ccy]
        start = df_ccy["as_of_date"].min().date()
        end = df_ccy["as_of_date"].max().date()
        tenors = sorted(df_ccy["tenor"].unique())
        print(f"{ccy}: {start} → {end} | tenors={tenors}")

    print("\nAll data overall:")
    print(df["as_of_date"].min().date(), "→", df["as_of_date"].max().date())

if __name__ == "__main__":
    main()

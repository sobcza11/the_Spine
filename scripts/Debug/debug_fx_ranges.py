from __future__ import annotations
import pandas as pd
from common.r2_client import read_parquet_from_r2

KEY = "spine_us/us_fx_spot_canonical.parquet"

def main():
    df = read_parquet_from_r2(KEY)
    df["fx_date"] = pd.to_datetime(df["fx_date"])

    print("\n=== FX SPOT DATE RANGES ===\n")
    for pair in sorted(df["pair"].unique()):
        df_p = df[df["pair"] == pair]
        start = df_p["fx_date"].min().date()
        end = df_p["fx_date"].max().date()
        print(f"{pair}: {start} → {end}")

    print("\nAll data overall:")
    print(df["fx_date"].min().date(), "→", df["fx_date"].max().date())

if __name__ == "__main__":
    main()



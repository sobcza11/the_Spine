import pandas as pd
from common.r2_client import read_parquet_from_r2

KEY = "spine_us/us_ir_diff_canonical.parquet"

def main():
    df = read_parquet_from_r2(KEY)
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])

    print("\n=== IR DIFF DATE RANGES ===\n")
    for pair in sorted(df["pair"].unique()):
        d = df[df["pair"] == pair]
        print(f"{pair}: {d['as_of_date'].min().date()} → {d['as_of_date'].max().date()}")

if __name__ == "__main__":
    main()


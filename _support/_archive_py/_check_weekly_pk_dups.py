import pandas as pd
df = pd.read_parquet("data/cot/cot_store_weekly_unified.parquet")

key = ["report_date","market_key","cot_format"]
if "trader_group" in df.columns:
    key2 = key + ["trader_group"]
else:
    key2 = key

dups = df.duplicated(key2).sum()
print("PK candidate:", key2)
print("duplicate rows:", dups)

if dups:
    print(df[df.duplicated(key2, keep=False)].sort_values(key2).head(20))

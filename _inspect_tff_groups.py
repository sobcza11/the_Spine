import pandas as pd

df = pd.read_parquet("data/cot/cot_store_weekly_unified.parquet")
tff = df[df["cot_format"].astype(str)=="tff"].copy()

print("TFF rows:", len(tff))
print("TFF date min/max:", tff["report_date"].min(), tff["report_date"].max())

tff["trader_group"] = tff["trader_group"].astype(str).str.strip()
print("\nUnique trader_group (count):")
print(tff["trader_group"].value_counts().head(30))

print("\nAll unique trader_group:")
print(sorted(tff["trader_group"].unique().tolist()))

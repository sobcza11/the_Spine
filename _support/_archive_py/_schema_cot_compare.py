import pandas as pd

tff_path   = "data/spine_us/us_cftc_cot_tff_canonical.parquet"
block4_path= "data/cot/block4/cot_block4_1986_1989_weekly_std.parquet"

tff = pd.read_parquet(tff_path)
b4  = pd.read_parquet(block4_path)

print("\nTFF cols:", len(tff.columns))
print(sorted(tff.columns))

print("\nBlock4 cols:", len(b4.columns))
print(sorted(b4.columns))

print("\nOnly in TFF:", sorted(set(tff.columns) - set(b4.columns))[:80])
print("\nOnly in Block4:", sorted(set(b4.columns) - set(tff.columns))[:80])

# show likely join keys if present
for c in ["as_of_date","report_date","cftc_contract_market_code","cftc_market_code","cftc_commodity_code","market_and_exchange_names","market"]:
    print("\n", c, "in TFF?", c in tff.columns, "| in Block4?", c in b4.columns)

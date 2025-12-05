import pandas as pd

P = "data/cot/block4/cot_block4_1986_1989_monthly.parquet"
df = pd.read_parquet(P)

# market_key = contract market code (string) for DEA legacy
df["market_key"] = df["cftc_contract_market_code"].astype(str).str.strip()

df.to_parquet(P, index=False)
print("patched:", P)
print("market_key nulls:", int(df["market_key"].isna().sum()))
print("sample market_key:", df["market_key"].head(10).tolist())

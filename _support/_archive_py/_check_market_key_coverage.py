import pandas as pd
df = pd.read_parquet("data/cot/cot_store_weekly_unified.parquet")

print(df.groupby("cot_format")["market_key"].nunique())
print("\nKey column presence by format:")
for fmt, g in df.groupby("cot_format"):
    print("\n---", fmt, "---")
    for c in ["spine_symbol","cftc_market_code","cftc_contract_market_code"]:
        print(c, "non-null:", g[c].notna().sum() if c in g.columns else 0)

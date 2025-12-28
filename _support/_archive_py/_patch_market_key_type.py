import pandas as pd

p = "data/cot/cot_store_weekly_unified.parquet"
df = pd.read_parquet(p)

def key_type(row):
    if row.get("cot_format") == "tff":
        # market_key came from spine_symbol (your precedence)
        return "spine_symbol"
    if row.get("cot_format") == "dea_legacy":
        return "cftc_contract_market_code"
    return "unknown"

df["market_key_type"] = df.apply(key_type, axis=1)

front = [c for c in ["report_date","market_key","market_key_type","cot_format","block_id","support_only","eligible_for_state","schema_version","built_at_utc"] if c in df.columns]
rest  = [c for c in df.columns if c not in front]
df = df[front + rest]

df.to_parquet(p, index=False)
print("saved:", p)
print(df["market_key_type"].value_counts())

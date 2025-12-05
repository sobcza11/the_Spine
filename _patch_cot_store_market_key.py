import pandas as pd
from datetime import datetime, timezone

p = "data/cot/cot_store_weekly_unified.parquet"
df = pd.read_parquet(p)

# canonical market key
def mk(row):
    if pd.notna(row.get("spine_symbol")) and str(row["spine_symbol"]).strip() != "":
        return str(row["spine_symbol"]).strip()
    if pd.notna(row.get("cftc_market_code")) and str(row["cftc_market_code"]).strip() != "":
        return str(row["cftc_market_code"]).strip()
    if pd.notna(row.get("cftc_contract_market_code")) and str(row["cftc_contract_market_code"]).strip() != "":
        return str(row["cftc_contract_market_code"]).strip()
    return None

df["market_key"] = df.apply(mk, axis=1)

# governance
df["schema_version"] = "cot_store_weekly_unified_v1"
df["built_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# keep key cols near front
front = [c for c in ["report_date","market_key","cot_format","block_id","support_only","eligible_for_state","schema_version","built_at_utc"] if c in df.columns]
rest  = [c for c in df.columns if c not in front]
df = df[front + rest]

df.to_parquet(p, index=False)

print("saved:", p)
print("cols:", len(df.columns))
print("market_key nulls:", df["market_key"].isna().sum())
print("sample market_key:", df["market_key"].dropna().head(10).tolist())

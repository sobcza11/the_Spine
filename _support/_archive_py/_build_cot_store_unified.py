import pandas as pd

tff_path   = "data/spine_us/us_cftc_cot_tff_canonical.parquet"
block4_path= "data/cot/block4/cot_block4_1986_1989_weekly_std.parquet"
out_path   = "data/cot/cot_store_weekly_unified.parquet"

tff = pd.read_parquet(tff_path)
b4  = pd.read_parquet(block4_path)

# 1) normalize date column name
if "as_of_date" in tff.columns and "report_date" not in tff.columns:
    tff = tff.rename(columns={"as_of_date":"report_date"})

# 2) add governance fields to tff so schema aligns
for col, default in [("block_id", 999), ("support_only", False), ("eligible_for_state", True)]:
    if col not in tff.columns:
        tff[col] = default

# 3) enforce key code fields as STRING (protect leading zeros & pyarrow issues)
for c in ["cftc_contract_market_code", "cftc_market_code", "cftc_commodity_code"]:
    if c in tff.columns:
        tff[c] = tff[c].astype(str).str.strip()
    if c in b4.columns:
        b4[c] = b4[c].astype(str).str.strip()

# 4) union on common columns (prevents accidental new/old mismatch)
common = sorted(set(tff.columns).intersection(set(b4.columns)))
tff2 = tff[common].copy()
b42  = b4[common].copy()

merged = pd.concat([b42, tff2], ignore_index=True)
merged["report_date"] = pd.to_datetime(merged["report_date"])
merged = merged.sort_values(["report_date"]).reset_index(drop=True)

merged.to_parquet(out_path, index=False)

print("saved:", out_path)
print("rows:", len(merged), "min:", merged["report_date"].min(), "max:", merged["report_date"].max())
print("block_id counts:\n", merged["block_id"].value_counts().sort_index())

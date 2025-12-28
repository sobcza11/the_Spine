import pandas as pd

tff_path   = "data/spine_us/us_cftc_cot_tff_canonical.parquet"
block4_path= "data/cot/block4/cot_block4_1986_1989_weekly_std.parquet"
out_path   = "data/cot/cot_store_weekly_unified.parquet"

tff = pd.read_parquet(tff_path)
b4  = pd.read_parquet(block4_path)

# normalize date
if "as_of_date" in tff.columns and "report_date" not in tff.columns:
    tff = tff.rename(columns={"as_of_date":"report_date"})

# tag source (this matters later for factor logic)
b4["cot_format"]  = "dea_legacy"
tff["cot_format"] = "tff"

# governance fields
if "block_id" not in b4.columns:  b4["block_id"] = 4
if "block_id" not in tff.columns: tff["block_id"] = 5  # treat TFF era as Block 5 (adjust later if you prefer)

for col, default in [("support_only", False), ("eligible_for_state", True)]:
    if col not in b4.columns:  b4[col] = default
    if col not in tff.columns: tff[col] = default

# enforce datetime
b4["report_date"]  = pd.to_datetime(b4["report_date"])
tff["report_date"] = pd.to_datetime(tff["report_date"])

# UNION concat (keeps all columns)
merged = pd.concat([b4, tff], ignore_index=True, sort=False)

# optional: move key cols to front if present
front = [c for c in ["report_date","cot_format","block_id","support_only","eligible_for_state"] if c in merged.columns]
rest  = [c for c in merged.columns if c not in front]
merged = merged[front + rest]

merged.to_parquet(out_path, index=False)

print("saved:", out_path)
print("rows:", len(merged), "cols:", len(merged.columns))
print("min:", merged["report_date"].min(), "max:", merged["report_date"].max())
print("cot_format counts:\n", merged["cot_format"].value_counts())
print("block_id counts:\n", merged["block_id"].value_counts().sort_index())

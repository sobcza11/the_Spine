import pandas as pd

p = "data/cot/block4/cot_block4_1986_1989_monthly.parquet"
df = pd.read_parquet(p)

# If month already exists, keep it. Otherwise try to derive/rename.
if "month" not in df.columns:
    # 1) derive from report_date if present
    if "report_date" in df.columns:
        df["month"] = pd.to_datetime(df["report_date"]).dt.to_period("M").dt.to_timestamp("M")
    else:
        # 2) if there is any datetime col, pick first as month-like & normalize
        dtcols = list(df.select_dtypes(include=["datetime64[ns]"]).columns)
        if dtcols:
            df["month"] = pd.to_datetime(df[dtcols[0]]).dt.to_period("M").dt.to_timestamp("M")
        else:
            raise ValueError("No report_date & no datetime columns found to derive month.")

# enforce datetime
df["month"] = pd.to_datetime(df["month"])

df.to_parquet(p, index=False)

print("patched:", p)
print("month min/max:", df["month"].min(), df["month"].max())
print("cols:", df.columns.tolist())

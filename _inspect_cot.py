import pandas as pd

paths = [
    "data/spine_us/us_cftc_cot_factors_canonical.parquet",
    "data/spine_us/us_cftc_cot_tff_canonical.parquet",
    "data/cot/block4/cot_block4_1986_1989_weekly_std.parquet",
]

for p in paths:
    df = pd.read_parquet(p)
    print("\n===", p, "===")
    print("rows:", len(df), "cols:", len(df.columns))

    dtcols = list(df.select_dtypes(include=["datetime64[ns]"]).columns)
    if dtcols:
        print("datetime cols:", dtcols)
        print("date min/max:", df[dtcols].min().min(), df[dtcols].max().max())
    else:
        print("datetime cols: NONE")

    for c in ["report_date","date","asof","block_id","support_only","eligible_for_state"]:
        if c in df.columns:
            print(c, "exists")

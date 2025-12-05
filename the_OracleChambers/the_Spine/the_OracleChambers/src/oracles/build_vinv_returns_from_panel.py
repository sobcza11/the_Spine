import pandas as pd
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[3]  # the_Spine root
DATA_DIR = ROOT / "data" / "vinv"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# This is the 3-col file you already have
panel_path = DATA_DIR / "vinv_monthly_panel.parquet"
df = pd.read_parquet(panel_path)

print("Loaded 3-col panel:", df.shape)
print("Columns:", list(df.columns))

# Rename to Phase 2 schema
df_ret = df.rename(columns={
    "ticker": "symbol",
    "month": "date",
    "monthly_ret": "ret",
}).copy()

# Ensure date is datetime
df_ret["date"] = pd.to_datetime(df_ret["date"])

# TODO: real benchmark later. For now set bench_ret = 0 so excess_ret = ret.
df_ret["bench_ret"] = 0.0

ret_path = DATA_DIR / "vinv_returns_monthly.parquet"
df_ret.to_parquet(ret_path, index=False)

print(f"Saved returns → {ret_path}")
print(df_ret.head())

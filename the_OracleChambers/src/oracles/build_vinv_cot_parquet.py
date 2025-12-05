import pandas as pd
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[3]
DATA_DIR = ROOT / "data" / "vinv"

csv_path = DATA_DIR / "vinv_cot_weekly_raw.csv"
out_path = DATA_DIR / "vinv_cot_weekly.parquet"

df = pd.read_csv(csv_path, parse_dates=["date"])
df.to_parquet(out_path, index=False)

print("Wrote COT parquet:", out_path, df.shape)


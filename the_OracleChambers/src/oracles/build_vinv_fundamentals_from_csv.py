import pandas as pd
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[3]
DATA_DIR = ROOT / "data" / "vinv"

csv_path = DATA_DIR / "vinv_fundamentals_monthly_template.csv"
out_parquet = DATA_DIR / "vinv_fundamentals_monthly.parquet"

df = pd.read_csv(csv_path, parse_dates=["date"])
df.to_parquet(out_parquet, index=False)

print("Updated fundamentals parquet from CSV:")
print(out_parquet, df.shape)


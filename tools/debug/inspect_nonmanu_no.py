from pathlib import Path
import pandas as pd

ROOT = Path.cwd()

path = ROOT / "data" / "ism" / "us_ism_nonmanu_no_by_industry_canonical.parquet"

if not path.exists():
    print("[MISSING]")
    raise SystemExit

df = pd.read_parquet(path)

print("\nSHAPE:\n")
print(df.shape)

print("\nCOLUMNS:\n")
print(df.columns.tolist())

print("\nHEAD:\n")
print(df.head())

date_col = "date" if "date" in df.columns else "as_of_date"

print("\nDATE RANGE:\n")
print(df[date_col].min(), "->", df[date_col].max()) 


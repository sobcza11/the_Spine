from pathlib import Path
import pandas as pd

ROOT = Path.cwd()

RAW = ROOT / "data/fundamentals/simfin/raw/cash_flow_statements.csv"
OUT = ROOT / "data/fundamentals/simfin/cashflow_quarterly.parquet"

df = pd.read_csv(RAW)

df["symbol"] = df["Ticker"].astype(str).str.upper()
df["statement_date"] = pd.to_datetime(df["Report Date"], errors="coerce")
df["fiscal_period"] = df["Fiscal Period"]

# Keep quarterly rows only if present; otherwise keep all.
q = df[df["Fiscal Period"].astype(str).str.upper().str.startswith("Q")].copy()
if len(q):
    df = q

df.to_parquet(OUT, index=False)

print("REBUILT:", OUT)
print("ROWS:", len(df))
print("SYMBOLS:", df["Ticker"].nunique())
print("MIN_DATE:", df["statement_date"].min())
print("MAX_DATE:", df["statement_date"].max())
print("COLUMNS:")
print(df.columns.tolist())


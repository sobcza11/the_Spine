# src\spine\jobs\fx_depth\pull_gold_fred.py
from pathlib import Path
import pandas as pd

REPO_ROOT = Path.cwd()
OUT = REPO_ROOT / "data" / "fx" / "fx_depth" / "raw" / "gold.parquet"

FRED_CSV = (
    "https://fred.stlouisfed.org/graph/fredgraph.csv"
    "?id=GOLDPMGBD228NLBM"
)

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(FRED_CSV)
    df = df.rename(columns={
        "observation_date": "date",
        "GOLDPMGBD228NLBM": "value"
    })

    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = (
        df[["date", "value"]]
        .dropna()
        .sort_values("date")
        .reset_index(drop=True)
    )

    df.to_parquet(OUT, index=False)

    print(f"BUILT: {OUT}")
    print(f"ROWS: {len(df)}")
    print(f"AS OF: {df['date'].max().date()}")
    print(f"LATEST GOLD USD/OZ: {df['value'].iloc[-1]:.2f}")

if __name__ == "__main__":
    main()
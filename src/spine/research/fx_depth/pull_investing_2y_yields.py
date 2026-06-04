from pathlib import Path
import pandas as pd
from investiny import search_assets, historical_data

OUT_DIR = Path.cwd() / "data" / "fx" / "fx_depth" / "raw" / "yields"
OUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = "01/01/2000"
END_DATE = "06/03/2026"

BOND_QUERIES = {
    "de_2y": "Germany 2-Year Bond Yield",
    "uk_2y": "United Kingdom 2-Year Bond Yield",
    "ca_2y": "Canada 2-Year Bond Yield",
    "jp_2y": "Japan 2-Year Bond Yield",
    "ch_2y": "Switzerland 2-Year Bond Yield"
}

all_data = {}

def normalize_investiny_rows(rows, name):
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    date_col = "date" if "date" in df.columns else "Date"
    value_col = (
        "close" if "close" in df.columns else
        "Close" if "Close" in df.columns else
        "price" if "price" in df.columns else
        "Price"
    )

    df = df.rename(columns={date_col: "date", value_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["series"] = name

    return (
        df[["date", "value", "series"]]
        .dropna(subset=["date"])
        .sort_values("date")
        .reset_index(drop=True)
    )

for name, query in BOND_QUERIES.items():
    try:
        print(f"\nSEARCHING: {query}")

        results = search_assets(
            query=query,
            limit=5,
            type="Bond"
        )

        if not results:
            print(f"FAIL {name}: no search results")
            continue

        print("CANDIDATES:")
        for idx, item in enumerate(results):
            print(idx, item)

        selected = results[0]
        investing_id = selected.get("ticker") or selected.get("id")

        if investing_id is None:
            print(f"FAIL {name}: no investing id found")
            continue

        print(f"SELECTED {name}: {investing_id}")

        rows = historical_data(
            investing_id=investing_id,
            from_date=START_DATE,
            to_date=END_DATE
        )

        df = normalize_investiny_rows(rows, name)

        if df.empty:
            print(f"FAIL {name}: empty dataframe")
            continue

        out_file = OUT_DIR / f"{name}.parquet"
        df.to_parquet(out_file, index=False)

        all_data[name] = df

        print(f"OK {name:<8} {len(df):>8,} rows -> {out_file}")

    except Exception as e:
        print(f"FAIL {name}: {e}")

print("\n" + "=" * 80)
print("HEAD(3)")
print("=" * 80)

for name, df in all_data.items():
    print("\n" + name.upper())
    print(df.head(3))

print("\n" + "=" * 80)
print("TAIL(3)")
print("=" * 80)

for name, df in all_data.items():
    print("\n" + name.upper())
    print(df.tail(3))

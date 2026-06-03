from pathlib import Path
import json
import pandas as pd

REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data" / "wti" / "us_wti_inventory_canonical.parquet"
OUTPUT_PATH = REPO_ROOT / "data" / "serving" / "fx" / "usdcad_wti_inventory_depth.json"

EXCLUDED_YEARS = {1997, 2003, 2008, 2014}
ROLLING_SURPLUS_YEARS = 3

def build_usdcad_wti_inventory_depth():
    df = pd.read_parquet(INPUT_PATH).copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = (
        df.dropna(subset=["date", "value"])
          .sort_values("date")
          .reset_index(drop=True)
    )

    df["year"] = df["date"].dt.year
    df["week"] = df["date"].dt.isocalendar().week.astype(int)

    df = df[df["week"].between(1, 52)].copy()
    df = df[~df["year"].isin(EXCLUDED_YEARS)].copy()

    rows = []

    for _, row in df.iterrows():
        year = int(row["year"])
        week = int(row["week"])
        value = float(row["value"])

        trailing_3yr = df[
            (df["week"] == week) &
            (df["year"] >= year - ROLLING_SURPLUS_YEARS) &
            (df["year"] < year)
        ]

        full_hist = df[
            (df["week"] == week) &
            (df["year"] < year)
        ]

        if trailing_3yr.empty or full_hist.empty:
            continue

        avg_3yr = float(trailing_3yr["value"].mean())
        hist_avg = float(full_hist["value"].mean())
        hist_min = float(full_hist["value"].min())
        hist_max = float(full_hist["value"].max())

        surplus_3yr = value - avg_3yr

        denom = hist_max - hist_min
        weekly_index = 100.0 if denom == 0 else ((value - hist_min) / denom) * 100.0

        rows.append({
            "date": row["date"].strftime("%Y-%m-%d"),
            "week": week,
            "year": year,
            "value": round(weekly_index, 4),
            "inventory_mmbbl": round(value, 4),
            "inventory_surplus_3yr": round(surplus_3yr, 4),
            "avg_3yr": round(avg_3yr, 4),
            "hist_avg": round(hist_avg, 4),
            "hist_min": round(hist_min, 4),
            "hist_max": round(hist_max, 4),
            "excluded_years": sorted(EXCLUDED_YEARS)
        })

    payload = {
        "pair": "USD/CAD",
        "metric": "WTI Inv.",
        "source": "EIA WCESTUS1 | the_Spine",
        "method": "3-year surplus + full-history weekly index after exclusions",
        "excluded_years": sorted(EXCLUDED_YEARS),
        "as_of_date": rows[-1]["date"] if rows else None,
        "rows": rows
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"BUILT: {OUTPUT_PATH}")
    print(f"ROWS: {len(rows)}")
    print(f"AS OF: {payload['as_of_date']}")

if __name__ == "__main__":
    build_usdcad_wti_inventory_depth()
    
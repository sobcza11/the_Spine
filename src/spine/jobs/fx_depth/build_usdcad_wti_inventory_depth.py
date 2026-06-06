from pathlib import Path
import json
import pandas as pd

REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data" / "wti" / "us_wti_inventory_canonical.parquet"
OUTPUT_PATH = REPO_ROOT / "data" / "serving" / "fx" / "usdcad_wti_inventory_depth.json"

EXCLUDED_YEARS = {1997, 2003, 2008, 2014}
ROLLING_SURPLUS_YEARS = 3
SEASONAL_OVERLAY_YEARS = 5

def safe_index(value, base):
    if base is None or base == 0 or pd.isna(base):
        return None
    return (value / base) * 100.0


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

    week1_by_year = (
        df[df["week"] == 1]
        .sort_values("date")
        .groupby("year")["value"]
        .first()
        .to_dict()
    )

    df["week1_value"] = df["year"].map(week1_by_year)
    df["seasonal_index_wk1"] = (df["value"] / df["week1_value"]) * 100.0
    df = df[df["seasonal_index_wk1"].notna()].copy()

    year = int(df["year"].max())

    current_year = (
        df[df["year"] == year]
        .sort_values("week")
        .copy()
    )

    output_rows = []

    for _, row in current_year.iterrows():
        week = int(row["week"])

        seasonal_window = df[
            (df["week"] == week) &
            (df["year"] >= year - SEASONAL_OVERLAY_YEARS) &
            (df["year"] < year)
        ]

        seasonal_hist = df[
            (df["week"] == week) &
            (df["year"] < year)
        ]

        value_index_wk1 = float(row["seasonal_index_wk1"])

        overlay_source = (
            seasonal_window
            if not seasonal_window.empty
            else seasonal_hist
        )

        hist_avg_index_wk1 = float(
            seasonal_hist["seasonal_index_wk1"].mean()
        )

        hist_min_index_wk1 = float(
            overlay_source["seasonal_index_wk1"].min()
        )

        hist_max_index_wk1 = float(
            overlay_source["seasonal_index_wk1"].max()
        )

        output_rows.append({
            "date": row["date"].strftime("%Y-%m-%d"),
            "week": week,
            "year": year,
            "value": round(value_index_wk1, 4),
            "inventory_mmbbl": round(float(row["value"]), 4),
            "inventory_display": f"{float(row['value']):.1f}k inv.",
            "hist_avg_index_wk1": round(hist_avg_index_wk1, 4),
            "hist_min_index_wk1": round(hist_min_index_wk1, 4),
            "hist_max_index_wk1": round(hist_max_index_wk1, 4),
        })

    output_rows = [
        r for r in output_rows
        if r["value"] is not None
    ]

    latest_date = current_year["date"].max()

    payload = {
        "pair": "USD/CAD",
        "metric": "WTI Inv.",
        "source": "EIA WCESTUS1 | the_Spine",
        "method": "Week-1 indexed seasonal WTI inventory growth + 5-year high-low/full-history average overlays",
        "index_base": "Week 1 = 100",
        "excluded_years": sorted(EXCLUDED_YEARS),
        "as_of_date": latest_date.strftime("%Y-%m-%d"),
        "rows": output_rows
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"BUILT: {OUTPUT_PATH}")
    print(f"ROWS: {len(output_rows)}")
    print(f"AS OF: {payload['as_of_date']}")


if __name__ == "__main__":
    build_usdcad_wti_inventory_depth()

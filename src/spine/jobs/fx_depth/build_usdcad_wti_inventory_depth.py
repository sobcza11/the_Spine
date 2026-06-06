from pathlib import Path
import json
import pandas as pd

REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data" / "wti" / "us_wti_inventory_canonical.parquet"
OUTPUT_PATH = REPO_ROOT / "data" / "serving" / "fx" / "usdcad_wti_inventory_depth.json"

EXCLUDED_YEARS = {1997, 2003, 2008, 2014}
ROLLING_SURPLUS_YEARS = 3


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

        std_3yr = float(trailing_3yr["value"].std(ddof=0))
        std_from_3yr_avg = 0.0 if std_3yr == 0 else surplus_3yr / std_3yr

        prior_rows = df[df["date"] < row["date"]].sort_values("date")
        prior_value = float(prior_rows.iloc[-1]["value"]) if len(prior_rows) else value

        if value > prior_value:
            inventory_direction = "↑"
        elif value < prior_value:
            inventory_direction = "↓"
        else:
            inventory_direction = "→"

        year_week1 = week1_by_year.get(year)

        trailing_week1_values = [
            week1_by_year.get(y)
            for y in range(year - ROLLING_SURPLUS_YEARS, year)
            if week1_by_year.get(y) is not None
        ]

        hist_week1_values = [
            week1_by_year.get(y)
            for y in sorted(week1_by_year)
            if y < year and week1_by_year.get(y) is not None
        ]

        avg_3yr_week1 = (
            sum(trailing_week1_values) / len(trailing_week1_values)
            if trailing_week1_values else None
        )

        hist_avg_week1 = (
            sum(hist_week1_values) / len(hist_week1_values)
            if hist_week1_values else None
        )

        hist_min_week1 = min(hist_week1_values) if hist_week1_values else None
        hist_max_week1 = max(hist_week1_values) if hist_week1_values else None

        value_index_wk1 = safe_index(value, year_week1)
        avg_3yr_index_wk1 = safe_index(avg_3yr, avg_3yr_week1)
        hist_avg_index_wk1 = safe_index(hist_avg, hist_avg_week1)
        hist_min_index_wk1 = safe_index(hist_min, hist_min_week1)
        hist_max_index_wk1 = safe_index(hist_max, hist_max_week1)

        rows.append({
            "date": row["date"].strftime("%Y-%m-%d"),
            "week": week,
            "year": year,

            "value": round(value_index_wk1, 4) if value_index_wk1 is not None else None,
            "inventory_mmbbl": round(value, 4),
            "inventory_display": f"{value:.1f}k inv.",
            "inventory_direction": inventory_direction,

            "inventory_surplus_3yr": round(surplus_3yr, 4),
            "std_3yr": round(std_3yr, 4),
            "std_from_3yr_avg": round(std_from_3yr_avg, 4),

            "year_week1": round(year_week1, 4) if year_week1 is not None else None,

            "avg_3yr": round(avg_3yr, 4),
            "hist_avg": round(hist_avg, 4),
            "hist_min": round(hist_min, 4),
            "hist_max": round(hist_max, 4),

            "avg_3yr_index_wk1": round(avg_3yr_index_wk1, 4) if avg_3yr_index_wk1 is not None else None,
            "hist_avg_index_wk1": round(hist_avg_index_wk1, 4) if hist_avg_index_wk1 is not None else None,
            "hist_min_index_wk1": round(hist_min_index_wk1, 4) if hist_min_index_wk1 is not None else None,
            "hist_max_index_wk1": round(hist_max_index_wk1, 4) if hist_max_index_wk1 is not None else None,

            "excluded_years": sorted(EXCLUDED_YEARS)
        })

    rows = [r for r in rows if r["value"] is not None]

    payload = {
        "pair": "USD/CAD",
        "metric": "WTI Inv.",
        "source": "EIA WCESTUS1 | the_Spine",
        "method": "Week-1 indexed seasonal WTI inventory growth + 3-year/full-history overlays",
        "index_base": "Week 1 = 100",
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

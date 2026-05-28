from pathlib import Path
import pandas as pd

REPO_ROOT = Path.cwd()

INFILE = REPO_ROOT / "data/spine_us/us_fx_spot_cross_t2.parquet"
OUTFILE = REPO_ROOT / "data/serving/fx/fx_zt_v1.parquet"
LATEST_JSON = REPO_ROOT / "data/serving/fx/fx_latest.json"


def expanding_z(s: pd.Series) -> pd.Series:
    return (s - s.expanding().mean()) / s.expanding().std()


def main():
    df = pd.read_parquet(INFILE).copy()
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)

    numeric_cols = [
        c for c in df.columns
        if c != "date" and pd.api.types.is_numeric_dtype(df[c])
    ]

    if not numeric_cols:
        raise ValueError("No numeric FX columns found.")

    df = (
        df[["date"] + numeric_cols]
        .groupby("date", as_index=False)
        .mean()
        .sort_values("date")
        .reset_index(drop=True)
    )

    z_cols = []
    for col in numeric_cols:
        z_col = f"{col}_z"
        df[z_col] = expanding_z(df[col])
        z_cols.append(z_col)

    df["fx_zt"] = df[z_cols].mean(axis=1).fillna(0)

    out = df[["date", "fx_zt"]].copy()

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTFILE, index=False)

    latest = out.tail(1).copy()
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")
    latest.to_json(LATEST_JSON, orient="records", indent=2)

    print("OK | FX Zt built + deduped")
    print(out.tail())
    print("rows:", len(out), "| unique dates:", out["date"].nunique())


if __name__ == "__main__":
    main()


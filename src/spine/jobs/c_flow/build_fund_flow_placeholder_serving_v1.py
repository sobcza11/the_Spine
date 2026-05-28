from pathlib import Path

import pandas as pd


def latest_date_from_sources(repo_root: Path):
    candidates = [
        repo_root / "data" / "serving" / "c_flow" / "c_flow_serving_v3.parquet",
        repo_root / "data" / "serving" / "fx" / "fx_serving_v2.parquet",
        repo_root / "data" / "serving" / "rates" / "rates_serving_v2.parquet",
        repo_root / "data" / "serving" / "geoscen" / "geoscen_serving_v2.parquet",
    ]

    dates = []

    for path in candidates:
        if not path.exists():
            continue

        df = pd.read_parquet(path)

        if "date" in df.columns and not df.empty:
            dates.append(pd.to_datetime(df["date"]).max())

    if not dates:
        return pd.Timestamp.today().normalize()

    return max(dates)


def main():
    repo_root = Path.cwd()

    out_path = repo_root / "data" / "serving" / "flows" / "fund_flow_serving_v1.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    latest_date = latest_date_from_sources(repo_root)

    out = pd.DataFrame([{
        "date": latest_date,
        "fund_flow_pressure": 0.0,
        "spy": 0.0,
        "qqq": 0.0,
        "hyg": 0.0,
        "tlt": 0.0,
        "ibit": 0.0,
        "source_status": "placeholder",
        "note": "Direct ETF / fund-flow source pending. Neutral placeholder used to prevent missing-flow warnings.",
    }])

    out.to_parquet(out_path, index=False)

    print("OK | fund flow placeholder serving v1")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
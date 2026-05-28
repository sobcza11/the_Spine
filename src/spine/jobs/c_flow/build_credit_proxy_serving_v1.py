from pathlib import Path
import pandas as pd


def main():
    repo_root = Path.cwd()

    fred_path = repo_root / "data" / "processed" / "rates" / "credit_proxy_inputs.parquet"
    out_path = repo_root / "data" / "serving" / "credit" / "credit_serving_v1.parquet"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(fred_path).copy()
    df["date"] = pd.to_datetime(df["date"])

    required_cols = [
        "baa_10y_spread_z",
        "hy_oas_z",
    ]

    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    df["credit_pressure"] = (
        df["baa_10y_spread_z"] +
        df["hy_oas_z"]
    ) / 2.0

    latest = df.sort_values("date").iloc[-1]

    out = pd.DataFrame([{
        "date": latest["date"],
        "baa_10y_spread_z": latest["baa_10y_spread_z"],
        "hy_oas_z": latest["hy_oas_z"],
        "credit_pressure": latest["credit_pressure"],
    }])

    out.to_parquet(out_path, index=False)

    print("OK | credit proxy serving v1")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
    
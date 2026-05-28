from pathlib import Path
import pandas as pd


ETF_COLUMNS = {
    "SPY": "spy_flow_z",
    "QQQ": "qqq_flow_z",
    "HYG": "hyg_flow_z",
    "TLT": "tlt_flow_z",
    "IBIT": "ibit_flow_z",
}


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "processed" / "flows" / "etf_flow_inputs.parquet"
    out_path = repo_root / "data" / "serving" / "flows" / "fund_flow_serving_v1.parquet"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(in_path).copy()
    df["date"] = pd.to_datetime(df["date"])

    missing = [v for v in ETF_COLUMNS.values() if v not in df.columns]

    if missing:
        raise KeyError(f"Missing ETF flow columns: {missing}")

    df["fund_flow_pressure"] = (
        df[list(ETF_COLUMNS.values())].sum(axis=1)
        / len(ETF_COLUMNS)
    )

    latest = df.sort_values("date").iloc[-1]

    out = pd.DataFrame([{
        "date": latest["date"],
        "fund_flow_pressure": latest["fund_flow_pressure"],
        **{k.lower(): latest[v] for k, v in ETF_COLUMNS.items()}
    }])

    out.to_parquet(out_path, index=False)

    print("OK | fund flow proxy serving v1")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
    
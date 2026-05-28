from pathlib import Path

import pandas as pd


def zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")

    std = s.std()

    if std == 0 or pd.isna(std):
        return s * 0

    return (s - s.mean()) / std


def main():
    repo_root = Path.cwd()

    in_path = (
        repo_root
        / "data"
        / "cot"
        / "btc_futures_cot_raw.csv"
    )

    out_path = (
        repo_root
        / "data"
        / "serving"
        / "cot"
        / "btc_futures_cot_serving_v1.parquet"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(
            f"Missing BTC futures COT file: {in_path}"
        )

    df = pd.read_csv(in_path).copy()

    date_candidates = [
        "date",
        "report_date",
        "as_of_date",
    ]

    date_col = None

    for c in date_candidates:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        raise KeyError(
            "No usable date column found."
        )

    df["date"] = pd.to_datetime(df[date_col])

    required_cols = [
        "leveraged_long",
        "leveraged_short",
        "open_interest",
    ]

    missing = [
        c for c in required_cols
        if c not in df.columns
    ]

    if missing:
        raise KeyError(
            f"Missing required BTC COT columns: {missing}"
        )

    df = df.sort_values("date").reset_index(drop=True)

    df["net_position"] = (
        df["leveraged_long"]
        - df["leveraged_short"]
    )

    df["net_position_oi_pct"] = (
        df["net_position"]
        / df["open_interest"]
    )

    df["net_position_z"] = zscore(
        df["net_position"]
    )

    df["oi_adjusted_z"] = zscore(
        df["net_position_oi_pct"]
    )

    df["position_momentum_4w"] = (
        df["net_position_z"]
        .diff(4)
    )

    df["crowding_score"] = (
        df["net_position_z"].abs()
        + df["oi_adjusted_z"].abs()
    ) / 2.0

    df["exhaustion_score"] = (
        (
            df["crowding_score"]
            * 0.6
        )
        +
        (
            df["position_momentum_4w"]
            .abs()
            * 0.4
        )
    )

    latest = df.iloc[-1]

    if latest["exhaustion_score"] >= 2.0:
        regime = "Extreme Crowding / Exhaustion"
    elif latest["exhaustion_score"] >= 1.0:
        regime = "Elevated Positioning"
    else:
        regime = "Balanced Positioning"

    out = df[
        [
            "date",
            "leveraged_long",
            "leveraged_short",
            "open_interest",
            "net_position",
            "net_position_oi_pct",
            "net_position_z",
            "oi_adjusted_z",
            "position_momentum_4w",
            "crowding_score",
            "exhaustion_score",
        ]
    ].copy()

    out["btc_cot_regime"] = regime

    out.to_parquet(out_path, index=False)

    print("OK | BTC futures COT pipeline v1")
    print(f"input: {in_path}")
    print(f"output: {out_path}")
    print("")

    print("latest:")
    print(
        out.tail(1).to_string(index=False)
    )


if __name__ == "__main__":
    main()


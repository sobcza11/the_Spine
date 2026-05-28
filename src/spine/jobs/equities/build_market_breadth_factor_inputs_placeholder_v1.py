from pathlib import Path

import pandas as pd


def latest_date(repo_root: Path):
    candidates = [
        repo_root / "data" / "serving" / "equities" / "equity_market_regime_v1.parquet",
        repo_root / "data" / "serving" / "c_flow" / "c_flow_serving_v4.parquet",
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

    out_path = (
        repo_root
        / "data"
        / "processed"
        / "equities"
        / "market_breadth_factor_inputs.parquet"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)

    date = latest_date(repo_root)

    out = pd.DataFrame(
        [
            {
                "date": date,
                "pct_above_50dma": 0.0,
                "pct_above_200dma": 0.0,
                "advance_decline_z": 0.0,
                "new_highs_lows_z": 0.0,
                "equal_weight_vs_cap_weight_z": 0.0,
                "growth_value_z": 0.0,
                "cyclical_defensive_z": 0.0,
                "momentum_factor_z": 0.0,
                "quality_factor_z": 0.0,
                "source_status": "placeholder",
                "note": "Neutral placeholder for Tier 1 breadth/factor engine. Replace with real breadth and factor data.",
            }
        ]
    )

    out.to_parquet(out_path, index=False)

    print("OK | market breadth/factor inputs placeholder v1")
    print(f"output: {out_path}")
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
    
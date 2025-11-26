import datetime as dt

from US_TeaPlant.bridges.fx_spot_bridge import build_fx_spot_window


def main() -> int:
    # 17 November 2025 → today
    start_date = dt.date(2025, 11, 17)
    end_date = dt.date.today()

    # During first test, keep this small to avoid hitting limits again.
    # Increase or drop limit_pairs once you see it working.
    df = build_fx_spot_window(start_date=start_date,
                              end_date=end_date,
                              limit_pairs=3)  # try 3 first: e.g., EURUSD, USDJPY, GBPUSD

    print(f"Rows: {len(df)}")
    print(f"Pairs: {df['pair'].nunique()}")
    print("Head:")
    print(df.head())
    print("Tail:")
    print(df.tail())
    print("Pairs present:", sorted(df['pair'].unique()))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())



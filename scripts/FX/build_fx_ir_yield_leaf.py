import datetime as dt

from US_TeaPlant.bridges.fx_ir_yield_bridge import (
    build_10y_yield_leaf,
    build_10y_yield_spreads,
)


def main() -> int:
    # You can tweak the window if desired
    start_date = dt.date(2000, 1, 1)
    end_date = None  # default = today

    print("[SCRIPT] Building 10Y yield leaf …")
    df_yld = build_10y_yield_leaf(start_date=start_date, end_date=end_date)

    print("[SCRIPT] Building 10Y yield spreads …")
    df_spreads = build_10y_yield_spreads(start_date=start_date, end_date=end_date)

    print(
        f"[SCRIPT] Done. 10Y yields rows={len(df_yld)}, "
        f"spreads rows={len(df_spreads)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


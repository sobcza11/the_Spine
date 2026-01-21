"""
spine.jobs.build_ir_diff_canonical

Job entrypoint that builds the canonical IR differential leaf.

This is the governed CI entrypoint:
    python -m spine.jobs.build_ir_diff_canonical

It delegates to:
    US_TeaPlant.bridges.ir_diff_bridge.build_ir_diff_canonical
"""

from __future__ import annotations

import argparse
import datetime as dt

from US_TeaPlant.bridges.ir_diff_bridge import build_ir_diff_canonical


def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build canonical IR diff leaf.")
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=dt.date(2000, 1, 1),
        help="Start date (YYYY-MM-DD). Default: 2000-01-01",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=dt.date.today(),
        help="End date (YYYY-MM-DD). Default: today",
    )
    args = parser.parse_args(argv)

    df = build_ir_diff_canonical(start_date=args.start, end_date=args.end)

    # Hard fail if nothing was produced (keeps CI honest)
    if df is None or getattr(df, "empty", True):
        raise SystemExit("[spine.jobs.build_ir_diff_canonical] No rows produced.")

    print(
        "[spine.jobs.build_ir_diff_canonical] OK "
        f"(rows={len(df):,}, pairs={df['pair'].nunique() if 'pair' in df.columns else 'n/a'})"
    )


if __name__ == "__main__":
    main()

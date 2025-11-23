"""
scripts/build_ir_yields_canonical.py

Orchestrates the IR yields stream build:

1) Fetch per-CCY yields & policy rates from free public APIs.
2) Build the canonical IR yields leaf and write it to R2.

Usage (from repo root):

    $env:PYTHONPATH = "$PWD\\src"
    python scripts/build_ir_yields_canonical.py
"""

from __future__ import annotations

import datetime as dt

from US_TeaPlant.bridges.ir_rates_bridge import build_ir_yields_canonical


def main() -> int:
    start_date = dt.date(2000, 1, 1)
    end_date = dt.date.today()

    print("[SCRIPT-IR] Building canonical IR yields leaf …")
    df_y = build_ir_yields_canonical(start_date=start_date, end_date=end_date)
    print(
        f"[SCRIPT-IR] IR yields canonical done: "
        f"rows={len(df_y):,}, ccy={df_y['ccy'].nunique() if not df_y.empty else 0}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


"""
scripts/build_ir_diff_canonical.py

Orchestrates the Interest Rate Differentials leaf build:

1) Read canonical IR yields leaf.
2) Read canonical FX spot leaf (for pair universe).
3) Build IR differentials per FX pair and write to R2.

Usage (from repo root):

    $env:PYTHONPATH = "$PWD\\src"
    python scripts/build_ir_diff_canonical.py
"""

from __future__ import annotations

import datetime as dt

from US_TeaPlant.bridges.ir_diff_bridge import build_ir_diff_canonical


def main() -> int:
    start_date = dt.date(2000, 1, 1)
    end_date = dt.date.today()

    print("[SCRIPT-IR] Building canonical IR diff leaf …")
    df_diff = build_ir_diff_canonical(start_date=start_date, end_date=end_date)
    print(
        f"[SCRIPT-IR] IR diff canonical done: "
        f"rows={len(df_diff):,}, pairs={df_diff['pair'].nunique() if not df_diff.empty else 0}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


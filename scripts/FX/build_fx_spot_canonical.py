"""
scripts/build_fx_spot_canonical.py

Orchestrates the FX stream build:

1) Refresh ECB EUR-reference FX history into R2.
2) Build the canonical FX spot leaf from that history.

Usage (from repo root):

    $env:PYTHONPATH = "$PWD\\src"
    python scripts/build_fx_spot_canonical.py
"""

from __future__ import annotations

import datetime as dt

from US_TeaPlant.bridges.fx_ecb_bridge import build_ecb_eur_fx_history
from US_TeaPlant.bridges.fx_spot_bridge import build_fx_spot_canonical_from_ecb


def main() -> int:
    start_date = dt.date(2000, 1, 1)
    end_date = dt.date.today()

    # 1) Build / refresh raw ECB EUR FX history into R2
    print("[SCRIPT-FX] Building ECB EUR FX history …")
    df_eur = build_ecb_eur_fx_history(start_date=start_date, end_date=end_date)
    print(
        f"[SCRIPT-FX] ECB EUR FX done: rows={len(df_eur)}, "
        f"ccy={df_eur['ccy'].nunique()}"
    )

    # 2) Build canonical FX spot leaf from that history
    print("[SCRIPT-FX] Building canonical FX spot leaf from ECB …")
    df_spot = build_fx_spot_canonical_from_ecb(
        start_date=start_date,
        end_date=end_date,
    )
    print(
        f"[SCRIPT-FX] FX spot canonical done: rows={len(df_spot)}, "
        f"pairs={df_spot['pair'].nunique()}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


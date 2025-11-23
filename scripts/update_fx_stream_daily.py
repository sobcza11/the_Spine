"""
update_fx_stream_daily.py

Daily FX refresh job for the_Spine – US_TeaPlant.

Intended to be run by a scheduler at 1:00 AM Monday–Saturday.
Steps:
  1. Refresh FRED FX raw history up to "yesterday".
  2. Rebuild the canonical FX spot leaf from the updated raw data.

Outputs in R2:
  - spine_us/us_fx_fed_raw.parquet
  - spine_us/us_fx_spot_canonical.parquet
"""

from __future__ import annotations

import datetime as dt

from US_TeaPlant.bridges.fx_fed_bridge import build_fed_fx_history
from US_TeaPlant.bridges.fx_spot_bridge import build_fx_spot_canonical


def main() -> int:
    # 1) Define our canonical window end: "yesterday"
    today = dt.date.today()
    end_date = today - dt.timedelta(days=1)

    print(f"[FX_STREAM] Refreshing FRED FX up to {end_date} …")
    df_raw = build_fed_fx_history(end_date=end_date)
    print(
        f"[FX_STREAM] FRED FX updated "
        f"(rows={len(df_raw)}, pairs={df_raw['pair'].nunique()})"
    )

    # 2) Rebuild canonical FX spot leaf
    print("[FX_STREAM] Rebuilding canonical FX spot leaf …")
    df_fx = build_fx_spot_canonical()
    print(
        f"[FX_STREAM] Canonical FX spot written "
        f"(rows={len(df_fx)}, pairs={df_fx['pair'].nunique()})"
    )

    print("[FX_STREAM] Daily FX Stream refresh complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



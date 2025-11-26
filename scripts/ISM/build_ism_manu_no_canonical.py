"""
scripts/build_ism_manu_no_canonical.py

Builds US ISM Manufacturing New Orders-by-industry canonical leaf
from data/ism/ism_pmi_transp.xlsx (sheet=manu_no).
"""

from __future__ import annotations

import os

from US_TeaPlant.bridges.ism_manu_no_bridge import (
    build_us_ism_manu_no_by_industry_canonical,
)


def main() -> int:
    excel_path = os.path.join("data", "ism", "ism_pmi_transp.xlsx")
    sheet_name = "manu_no"

    print("[SCRIPT-ISM-MANU-NO] Building US ISM Manu New Orders-by-industry …")
    df = build_us_ism_manu_no_by_industry_canonical(
        excel_path=excel_path,
        sheet_name=sheet_name,
    )
    print(
        f"[SCRIPT-ISM-MANU-NO] Done: rows={len(df)}, "
        f"sectors={df['sector_name'].nunique()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


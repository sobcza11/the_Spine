"""
scripts/build_ism_manu_pmi_rank_canonical.py

Builds US ISM Manufacturing PMI-rank-by-industry canonical leaf
from data/ism/ism_pmi_transp.xlsx (sheet=manu_rank).
"""

from __future__ import annotations

import os

from US_TeaPlant.bridges.ism_manu_rank_bridge import (
    build_us_ism_manu_pmi_rank_by_industry_canonical,
)


def main() -> int:
    excel_path = os.path.join("data", "ism", "ism_pmi_transp.xlsx")
    sheet_name = "manu_rank"

    print("[SCRIPT-ISM-MANU-RANK] Building US ISM Manu PMI-rank-by-industry …")
    df = build_us_ism_manu_pmi_rank_by_industry_canonical(
        excel_path=excel_path,
        sheet_name=sheet_name,
    )
    print(
        f"[SCRIPT-ISM-MANU-RANK] Done: rows={len(df)}, "
        f"sectors={df['sector_name'].nunique()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

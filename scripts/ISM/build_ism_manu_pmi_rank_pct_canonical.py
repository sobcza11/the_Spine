"""
scripts/build_ism_manu_pmi_rank_pct_canonical.py

Builds US ISM Manufacturing PMI-rank-percent-by-industry canonical leaf
from data/ism/ism_pmi_transp.xlsx (sheet=manu_rank_pct).
"""

from __future__ import annotations

import os

from US_TeaPlant.bridges.ism_manu_rank_pct_bridge import (
    build_us_ism_manu_pmi_rank_pct_by_industry_canonical,
)


def main() -> int:
    excel_path = os.path.join("data", "ism", "ism_pmi_transp.xlsx")
    sheet_name = "manu_rank_pct"

    print(
        "[SCRIPT-ISM-MANU-RANK-PCT] "
        "Building US ISM Manu PMI-rank-pct-by-industry …"
    )
    df = build_us_ism_manu_pmi_rank_pct_by_industry_canonical(
        excel_path=excel_path,
        sheet_name=sheet_name,
    )
    print(
        f"[SCRIPT-ISM-MANU-RANK-PCT] Done: rows={len(df)}, "
        f"sectors={df['sector_name'].nunique()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


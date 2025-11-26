"""
scripts/build_ism_manu_pmi_canonical.py

One-off / occasional builder for US ISM Manufacturing PMI-by-industry
from the local Excel workbook into the_Spine canonical leaf.

Usage (from repo root):

    $env:PYTHONPATH = "$PWD\\src"
    python scripts/build_ism_manu_pmi_canonical.py
"""

from __future__ import annotations

import os

from US_TeaPlant.bridges.ism_manu_pmi_bridge import (
    build_us_ism_manu_pmi_by_industry_canonical,
)


def main() -> int:
    excel_path = os.path.join("data", "ism", "ism_pmi_transp.xlsx")
    sheet_name = "manu_pmi"

    print("[SCRIPT-ISM-MANU-PMI] Building US ISM Manufacturing PMI-by-industry …")
    df = build_us_ism_manu_pmi_by_industry_canonical(
        excel_path=excel_path,
        sheet_name=sheet_name,
    )
    print(
        f"[SCRIPT-ISM-MANU-PMI] Done: rows={len(df)}, "
        f"sectors={df['sector_name'].nunique()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


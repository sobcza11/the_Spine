from __future__ import annotations
import os

from US_TeaPlant.bridges.ism_nonmanu_no_bridge import (
    build_us_ism_nonmanu_no_by_industry_canonical,
)


def main() -> int:
    excel_path = os.path.join("data", "ism", "ism_pmi_transp.xlsx")

    print("[SCRIPT-ISM-NONMANU-NO] Building Services New Orders-by-industry …")
    df = build_us_ism_nonmanu_no_by_industry_canonical(excel_path=excel_path)

    print(
        f"[SCRIPT-ISM-NONMANU-NO] Done: rows={len(df)}, "
        f"sectors={df['sector_name'].nunique()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


from datetime import datetime

import pandas as pd

from common.r2_client import read_parquet_from_r2, write_parquet_to_r2
from US_TeaPlant.bridges.wti_inv_stor_bridge import (
    WTIInventoryConfig,
    build_wti_leaf,
)

RAW_WTI_KEY = "raw/us/wti_inv_stor/df_wti_inv_stor_hist.parquet"
WTI_LEAF_KEY = "spine_us/us_wti_index_leaf.parquet"


def main() -> None:
    print(f"[wti_leaf_upload] Loading historical WTI from R2: {RAW_WTI_KEY}")
    df_hist = read_parquet_from_r2(RAW_WTI_KEY)
    print("[wti_leaf_upload] Raw columns:", list(df_hist.columns))

    # normalize raw schema
    df_hist["Date"] = pd.to_datetime(df_hist["Date"])
    df_hist["year"] = df_hist["Date"].dt.year
    df_hist["W#"] = df_hist["Date"].dt.isocalendar().week.astype(int)
    df_hist = df_hist.rename(columns={"INV": "wkly_inv"})

    cfg = WTIInventoryConfig()
    target_year = int(df_hist["year"].max())
    print(f"[wti_leaf_upload] Building WTI leaf for year={target_year}")

    wti_leaf = build_wti_leaf(df_hist, cfg, target_year)

    print("[wti_leaf_upload] Sample of WTI leaf:")
    print(wti_leaf.head(10))

    print(f"[wti_leaf_upload] Writing WTI leaf to R2: {WTI_LEAF_KEY}")
    write_parquet_to_r2(wti_leaf, WTI_LEAF_KEY)

    print("[wti_leaf_upload] Done at UTC:", datetime.utcnow())


if __name__ == "__main__":
    main()
    




"""
Update WTI historical inventory in R2 and rebuild Spine-US core.

Steps:
1) Load existing WTI history from R2
2) Fetch latest raw WTI data from source
3) Normalize + validate via WTI bridge
4) Append only new weeks (canonical Wednesday-based)
5) Write updated history back to R2
6) Rebuild Spine-US core (existing script)
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from common.r2_client import read_parquet_from_r2, write_parquet_to_r2
from US_TeaPlant.bridges.wti_inv_stor_bridge import (
    WTIInventoryConfig,
    normalize_wti_inventory_schema,
    validate_wti_inventory_schema,
    append_new_weeks,
)

# If your build script is in scripts/build_spine_us_core.py and "scripts"
# is a package, you can import the main function. Otherwise, adjust as needed.
from scripts.build_spine_us_core import main as build_spine_us_core


RAW_WTI_PATH = "raw/us/wti_inv_stor/df_wti_inv_stor_hist.parquet"


def fetch_latest_wti() -> pd.DataFrame:
    """
    Fetch latest WTI inventory data from your external source.

    TODO: Implement the actual ingestion logic here.
    Examples:
    - Read from CSV: pd.read_csv(os.environ["WTI_SOURCE_CSV"])
    - Call external API, parse JSON to DataFrame

    This function MUST return a DataFrame that contains the raw columns
    expected by your `normalize_wti_inventory_schema` function.
    """
    raise NotImplementedError(
        "Implement fetch_latest_wti() to pull from your real data source "
        "(CSV, API, database, etc.)."
    )


def main():
    cfg = WTIInventoryConfig()  # uses default lookback_years, min_year, max_year

    print("[update_wti] Starting WTI history + Spine-US update...")
    print(f"[update_wti] As of UTC: {datetime.utcnow()}")

    # 1) Load existing WTI history from R2
    print(f"[update_wti] Loading existing WTI history from R2: {RAW_WTI_PATH}")
    try:
        df_hist: pd.DataFrame = read_parquet_from_r2(RAW_WTI_PATH)
    except FileNotFoundError:
        print("[update_wti] No existing WTI history file found in R2. "
              "Treating this as an initial load.")
        df_hist = pd.DataFrame()

    # 2) Fetch latest raw WTI data from source
    print("[update_wti] Fetching latest WTI data from source...")
    df_new_raw = fetch_latest_wti()

    if df_new_raw is None or df_new_raw.empty:
        print("[update_wti] No new raw WTI data returned. Exiting without changes.")
        return

    # 3) Normalize + validate new data using the bridge logic
    print("[update_wti] Normalizing new WTI data...")
    df_new_norm = normalize_wti_inventory_schema(df_new_raw, cfg=cfg)

    print("[update_wti] Validating new WTI schema...")
    validate_wti_inventory_schema(df_new_norm, cfg=cfg)

    # 4) Append only new weeks based on canonical Wednesday
    print("[update_wti] Appending new weeks to historical data...")
    df_updated = append_new_weeks(df_hist, df_new_norm)

    # 5) Write updated WTI history back to R2
    print("[update_wti] Writing updated WTI history back to R2...")
    write_parquet_to_r2(df_updated, RAW_WTI_PATH)

    # 6) Rebuild Spine-US core using your existing script
    print("[update_wti] Rebuilding Spine-US core...")
    build_spine_us_core()

    print(
        "[update_wti] Done. WTI history & Spine-US core are up to date as of "
        f"{datetime.utcnow().date()}."
    )


if __name__ == "__main__":
    main()



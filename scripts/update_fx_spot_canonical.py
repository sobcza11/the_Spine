from __future__ import annotations

import datetime as dt
import pandas as pd

from common.r2_client import read_parquet_from_r2, write_parquet_to_r2
from US_TeaPlant.bridges.fx_fed_bridge import build_fed_fx_history
from US_TeaPlant.bridges.fx_cross_builder import build_cross_pairs_from_eur

CANONICAL_KEY = "spine_us/us_fx_spot_canonical.parquet"
FED_RAW_KEY = "spine_us/us_fx_fed_raw.parquet"
ECB_RAW_KEY = "spine_us/us_fx_ecb_eur_raw.parquet"


def update_fx_spot_canonical():
    print("[INFO] Loading existing canonical …")
    df_old = read_parquet_from_r2(CANONICAL_KEY)

    last_date = df_old["fx_date"].max().date()
    today = dt.date.today()

    print(f"[INFO] Last FX date is {last_date}, updating through {today} …")

    # Pull new FRED data
    df_new_fed = build_fed_fx_history(start_date=last_date, end_date=today)

    frames = [df_old, df_new_fed]

    # Optional ECB crosses
    try:
        df_eur = read_parquet_from_r2(ECB_RAW_KEY)
        df_eur_new = df_eur[df_eur["fx_date"].dt.date > last_date]
        if not df_eur_new.empty:
            df_cross_new = build_cross_pairs_from_eur(df_eur_new)
            frames.append(df_cross_new)
    except Exception:
        print("[WARN] ECB EUR raw missing — skipping EUR crosses.")

    df_all = pd.concat(frames, ignore_index=True)

    df_all["fx_date"] = pd.to_datetime(df_all["fx_date"])
    df_all = df_all.drop_duplicates(subset=["pair", "fx_date"])
    df_all = df_all.sort_values(["pair", "fx_date"], ascending=[True, False])

    write_parquet_to_r2(df_all, CANONICAL_KEY, index=False)
    print("[INFO] Canonical FX spot updated.")

    return df_all


def main() -> int:
    df = update_fx_spot_canonical()
    print(df.head())
    print(df.tail())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

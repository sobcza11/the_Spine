"""
Build canonical US FX weekly returns leaf for the_Spine.

Inputs:
    data/us/us_fx_spot_canonical.parquet
        Expected columns:
            as_of_date (datetime-like)
            symbol     (e.g. 'EURUSD', 'JPYUSD', etc.)
            price      (spot/fx level)

Outputs:
    data/spine_us/us_fx_returns_canonical.parquet
        Columns:
            as_of_date
            symbol
            ret_1w          (simple 1-week return)
            log_ret_1w      (log 1-week return)
"""

import os
import sys
import logging

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("SCRIPT-FX-RETURNS")

FX_LEVELS_PATH = "data/us/us_fx_spot_canonical.parquet"   # adjust if needed
OUTPUT_PATH    = "data/spine_us/us_fx_returns_canonical.parquet"


def load_fx_levels() -> pd.DataFrame:
    if not os.path.exists(FX_LEVELS_PATH):
        raise FileNotFoundError(
            f"FX levels file not found at {FX_LEVELS_PATH}. "
            "Adjust FX_LEVELS_PATH in build_us_fx_returns_canonical.py."
        )

    df = pd.read_parquet(FX_LEVELS_PATH)
    expected_cols = {"as_of_date", "symbol"}
    if not expected_cols.issubset(df.columns):
        raise ValueError(
            f"FX levels file must contain {expected_cols}, "
            f"got columns={list(df.columns)}"
        )

    # Try to infer the price column if not obvious
    price_cols = [c for c in df.columns if c.lower() in {"price", "close", "spot"}]
    if not price_cols:
        raise ValueError(
            "Could not infer FX price column (expected one of 'price', 'close', 'spot'). "
            f"Columns={list(df.columns)}"
        )
    price_col = price_cols[0]
    log.info("[FX-RETURNS] Using price column: %s", price_col)

    df = df[["as_of_date", "symbol", price_col]].rename(columns={price_col: "price"})
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


def compute_weekly_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute 1-week simple & log returns by symbol.
    Assumes df has daily or weekly observations.
    """
    df = df.copy()
    df = df.sort_values(["symbol", "as_of_date"])

    def _by_symbol(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("as_of_date")
        g["ret_1w"] = g["price"].pct_change()          # simple return
        g["log_ret_1w"] = np.log(g["price"]).diff()    # log return
        return g

    df_ret = df.groupby("symbol", group_keys=False).apply(_by_symbol)

    # Drop rows where returns are NaN for all symbols at first date
    df_ret = df_ret.dropna(subset=["ret_1w", "log_ret_1w"], how="all")
    return df_ret[["as_of_date", "symbol", "ret_1w", "log_ret_1w"]]


def main(argv=None) -> int:
    log.info("[FX-RETURNS] Building FX returns leaf …")

    df_levels = load_fx_levels()
    log.info(
        "[FX-RETURNS] Loaded FX levels: rows=%s, symbols=%s",
        df_levels.shape[0],
        df_levels["symbol"].nunique(),
    )

    df_ret = compute_weekly_returns(df_levels)
    log.info(
        "[FX-RETURNS] Computed returns: rows=%s, symbols=%s",
        df_ret.shape[0],
        df_ret["symbol"].nunique(),
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    log.debug("Writing FX returns to %s", OUTPUT_PATH)
    df_ret.to_parquet(OUTPUT_PATH, index=False)
    log.info("[FX-RETURNS] Wrote FX returns to %s", OUTPUT_PATH)

    log.info("[FX-RETURNS] Done – FX returns leaf built successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


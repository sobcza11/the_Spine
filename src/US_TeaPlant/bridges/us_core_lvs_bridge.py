"""
US Core Leaves Bridge

Aggregates governed US macro leaves (WTI, yield curve, etc.)
into a unified table for the Spine-US trunk.
"""

from __future__ import annotations

import logging
import pandas as pd

from common.r2_client import read_parquet_from_r2

logger = logging.getLogger(__name__)


def _load_wti_leaf_from_r2() -> pd.DataFrame:
    """
    Load the governed WTI leaf from R2.

    Expected key:
        spine_us/us_wti_index_leaf.parquet
    """
    logger.info("Loading WTI leaf from R2")
    df = read_parquet_from_r2("spine_us/us_wti_index_leaf.parquet")
    # wti_date → as_of_date
    df = df.rename_axis("as_of_date")
    return df


def _load_yc_leaf_from_r2() -> pd.DataFrame:
    """
    Load the governed YC spreads leaf from R2, if present.

    Expected key:
        spine_us/us_yc_spreads_leaf.parquet
    """
    logger.info("Loading YC spreads leaf from R2")
    df = read_parquet_from_r2("spine_us/us_yc_spreads_leaf.parquet")
    # yc_date → as_of_date
    df = df.rename_axis("as_of_date")
    return df


def build_us_core_leaves(include_yc: bool = True) -> pd.DataFrame:
    """
    Build the unified US core leaves table from governed leaves in R2.

    Current leaves:
        - WTI index envelope (required)
        - Yield curve spreads (10y–3m & 10y–2y) (optional)

    Returns
    -------
    pd.DataFrame
        US core leaves, indexed by as_of_date.
    """
    logger.info("Building US core leaves (include_yc=%s)", include_yc)

    # 1) WTI leaf (required)
    core = _load_wti_leaf_from_r2()

    # 2) Optional YC leaf
    if include_yc:
        try:
            yc_leaf = _load_yc_leaf_from_r2()
        except Exception as e:  # NoSuchKey, etc.
            logger.warning("YC leaf not available, continuing with WTI only: %s", e)
        else:
            core = core.join(yc_leaf, how="left")

    core.index.name = "as_of_date"
    return core







import pandas as pd
from dataclasses import dataclass

from common.r2_client import write_parquet_to_r2
from US_TeaPlant.bridges.us_core_lvs_bridge import build_us_core_leaves


@dataclass
class SpineUSCoreConfig:
    output_dir: str = "spine_us"
    output_filename: str = "us_spine_core.parquet"
    sort_index: bool = True


def build_spine_us_core(cfg: SpineUSCoreConfig) -> pd.DataFrame:
    """
    Build Spine-US core from all US leaves and persist to R2.

    Current leaves:
      - WTI index envelope (required)
      - YC spreads (10y–3m, 10y–2y) if available
    """
    # multi-leaf US core (WTI + YC if present)
    df_core = build_us_core_leaves(include_yc=True)

    if cfg.sort_index:
        df_core = df_core.sort_index()

    write_parquet_to_r2(
        df_core,
        f"{cfg.output_dir}/{cfg.output_filename}",
    )

    return df_core


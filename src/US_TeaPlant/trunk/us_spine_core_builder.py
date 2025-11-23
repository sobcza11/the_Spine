import pandas as pd
from dataclasses import dataclass

from common.r2_client import (
    read_parquet_from_r2,
    write_parquet_to_r2,
)


@dataclass
class SpineUSCoreConfig:
    output_dir: str = "spine_us"
    output_filename: str = "us_spine_core.parquet"
    sort_index: bool = True


def load_wti_leaf() -> pd.DataFrame:
    """
    Load WTI leaf from R2.
    """
    return read_parquet_from_r2("spine_us/us_wti_index_leaf.parquet")


def build_spine_us_core(cfg: SpineUSCoreConfig) -> pd.DataFrame:
    """
    Build Spine-US core (WTI-only for now) and persist to R2.
    """
    df_wti = load_wti_leaf()
    df_wti = df_wti.rename_axis("as_of_date")

    df_core = df_wti.copy()

    if cfg.sort_index:
        df_core = df_core.sort_index()

    write_parquet_to_r2(
        df_core,
        f"{cfg.output_dir}/{cfg.output_filename}",
    )

    return df_core



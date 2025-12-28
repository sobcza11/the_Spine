"""
GeoScen™ US leaf template.

This module defines the *shape* and minimal construction logic for the
GeoScen_US leaf, without requiring that long-history global datasets
are already integrated.

The intent is:
    - to provide a CPMAI-compliant, testable interface
    - to allow synthetic or partial data to flow through MAIN_p
    - to make it trivial to "drop in" real demographic / migration /
      housing datasets (1950+ and global) later.
"""

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

from .config import GeoScenConfig, default_geoscen_config


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass
class GeoScenLeafRow:
    """
    Single-row representation of GeoScen signals for a given date and region.

    All fields are intentionally numeric and interpretable, so they can be
    fused into MAIN_p without ambiguity.

    The fields are defined conceptually first; they can be populated from:
        - BeigeBook geography tags
        - migration statistics
        - housing market indicators
        - labor import/export metrics
        - demographic change estimates
    """

    as_of_date: date
    region_id: str

    # Core GeoScen pressures (dimensionless indices, e.g. z-scores).
    migration_pressure: float
    housing_turnover_rate: float
    housing_burden_index: float
    population_inflow_diffusion: float
    sectoral_labor_import_index: float
    demographic_shift_index: float

    # Composite "geographic scent" summary: higher => stronger local macro heat.
    geo_scen_composite: float


def _empty_leaf_for_config(config: GeoScenConfig) -> pd.DataFrame:
    """
    Generate a minimal, empty/synthetic GeoScen leaf for testing and wiring.

    This is useful when:
        - you want MAIN_p to have a placeholder GeoScen signal
        - you have not yet integrated real 1950+ datasets
    """
    if not config.supported_regions:
        raise ValueError("GeoScenConfig.supported_regions is empty.")

    # A single dummy date (e.g., today's date or a neutral placeholder).
    today = date.today()
    rows: List[GeoScenLeafRow] = []

    for region in config.supported_regions:
        rows.append(
            GeoScenLeafRow(
                as_of_date=today,
                region_id=region,
                migration_pressure=0.0,
                housing_turnover_rate=0.0,
                housing_burden_index=0.0,
                population_inflow_diffusion=0.0,
                sectoral_labor_import_index=0.0,
                demographic_shift_index=0.0,
                geo_scen_composite=0.0,
            )
        )

    df = pd.DataFrame([r.__dict__ for r in rows])
    return df


def build_geoscen_leaf(
    config: Optional[GeoScenConfig] = None,
    source_frames: Optional[Iterable[pd.DataFrame]] = None,
) -> pd.DataFrame:
    """
    Build a GeoScen_US leaf DataFrame.

    Current behavior (Phase I–III):
        - If no source_frames are provided, returns a neutral placeholder leaf:
            * one row per supported region
            * all indices set to 0.0
        - If source_frames are provided, this function can be extended to
          merge and normalize them into the GeoScen indices.

    Future behavior (Phase IV+):
        - Combine multi-decade demographic, migration, and housing series
          into stable, scale-aware indices
        - Support global regions beyond the US, with consistent schema
    """
    cfg = config or default_geoscen_config()

    if not source_frames:
        logger.info("No GeoScen source_frames provided; building neutral placeholder leaf.")
        leaf_df = _empty_leaf_for_config(cfg)
    else:
        logger.info("Building GeoScen leaf from provided source_frames (experimental).")

        # Phase I: concatenate and aggregate simple standardized signals.
        # This is a *template*; you will customize aggregation rules later.
        combined = pd.concat(source_frames, ignore_index=True)

        required_cols = {"as_of_date", "region_id"}
        if not required_cols.issubset(combined.columns):
            raise ValueError(
                f"GeoScen source_frames must include {required_cols}, "
                f"found columns={set(combined.columns)}"
            )

        # Ensure indices exist; fill missing ones with 0.0 for now.
        index_cols = [
            "migration_pressure",
            "housing_turnover_rate",
            "housing_burden_index",
            "population_inflow_diffusion",
            "sectoral_labor_import_index",
            "demographic_shift_index",
        ]
        for col in index_cols:
            if col not in combined.columns:
                logger.warning(f"Column {col} missing in source_frames; filling with 0.0")
                combined[col] = 0.0

        def _agg(group: pd.DataFrame) -> pd.Series:
            # Simple mean aggregation; can be upgraded to weighted schemes.
            series = {}
            for col in index_cols:
                series[col] = float(group[col].mean())
            # Composite is just an average for now; can be tuned later.
            vals = [series[c] for c in index_cols]
            series["geo_scen_composite"] = float(np.mean(vals)) if vals else 0.0
            return pd.Series(series)

        grouped = (
            combined.groupby(["as_of_date", "region_id"])
            .apply(_agg)
            .reset_index()
        )
        leaf_df = grouped

    return leaf_df


def save_geoscen_leaf(df: pd.DataFrame, config: Optional[GeoScenConfig] = None) -> Path:
    """
    Persist the GeoScen_US leaf to its configured parquet location.

    Returns:
        Path to the written parquet file.
    """
    cfg = config or default_geoscen_config()
    cfg.processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = cfg.processed_dir / cfg.leaf_filename
    df.to_parquet(out_path, index=False)
    logger.info(f"Saved GeoScen_US leaf to {out_path} with {len(df)} rows.")
    return out_path


def main() -> None:
    """
    Convenience CLI entry point.

    Phase I–III:
        - Build a neutral placeholder leaf and save it.

    This allows MAIN_p to start consuming a GeoScen leaf immediately,
    even before real long-history data sources are integrated.
    """
    cfg = default_geoscen_config()
    leaf_df = build_geoscen_leaf(cfg, source_frames=None)
    save_geoscen_leaf(leaf_df, cfg)


if __name__ == "__main__":
    main()

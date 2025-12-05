from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class GeoScenConfig:
    """
    Configuration for the GeoScen_US module.

    This is intentionally light and CPMAI-friendly:
    - It documents where GeoScen expects to find upstream signals
      (e.g., BeigeBook geography tags, migration stats, housing indicators).
    - It defines where the GeoScen leaf will be written.
    - It does NOT hardcode any repository-level paths.
    """

    # Where intermediate and final GeoScen artifacts live (relative to project root).
    processed_dir: Path = Path("data/processed/GeoScen_US")

    # Leaf filename (one row per date, optionally per region).
    leaf_filename: str = "geo_scen_leaf.parquet"

    # Placeholder list of US regions / districts that GeoScen will support.
    # In early phases, this may mirror BeigeBook district IDs.
    supported_regions: List[str] = None


def default_geoscen_config() -> GeoScenConfig:
    """
    Returns a default GeoScenConfig with typical US Fed-style regions.

    NOTE:
    - These labels are intentionally generic: they can represent BeigeBook
      districts, Census regions, or other place-based schemes.
    - When you onboard a specific dataset (e.g., 1950+ migration), you can
      adapt this list without breaking the core interface.
    """
    regions = [
        "Boston",
        "New_York",
        "Philadelphia",
        "Cleveland",
        "Richmond",
        "Atlanta",
        "Chicago",
        "St_Louis",
        "Minneapolis",
        "Kansas_City",
        "Dallas",
        "San_Francisco",
        "National",
    ]
    return GeoScenConfig(supported_regions=regions)

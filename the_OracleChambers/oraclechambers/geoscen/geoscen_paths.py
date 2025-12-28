from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass

@dataclass(frozen=True)
class GeoScenPaths:
    root: Path

def get_geoscen_root() -> Path:
    \"\"\"Resolve GeoScen root inside the_OracleChambers workspace.\"\"\"
    # This assumes this file lives at: the_OracleChambers/oraclechambers/geoscen/geoscen_paths.py
    return Path(__file__).resolve().parents[2] / "GeoScen"

def get_geoscen_us_root() -> Path:
    return get_geoscen_root() / "the_GeoScen_US"

from __future__ import annotations

from pathlib import Path
import pandas as pd

def build_geoscen_leaf_stub(asof: str) -> pd.DataFrame:
    \"\"\"Stub: returns an empty GeoScen leaf shell (context-only).\"\"\"
    return pd.DataFrame(
        [{
            "asof": asof,
            "geoscen_status": "stub",
            "note": "Contextual diagnostic leaf — not a scored FT-GMI pillar input."
        }]
    )

def write_geoscen_leaf_stub(out_path: Path, asof: str) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = build_geoscen_leaf_stub(asof=asof)
    df.to_parquet(out_path, index=False)
    return out_path

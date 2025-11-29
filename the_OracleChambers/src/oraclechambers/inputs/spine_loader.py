"""
Helpers to work directly with the_Spine outputs.

Most consumers should prefer oraclechambers.registry.build_context(),
but these functions are useful for exploration and notebooks.
"""

from typing import Optional
import pandas as pd
from ..config import SPINE_DATA_DIR


def load_macro_state_spine_us() -> Optional[pd.DataFrame]:
    path = SPINE_DATA_DIR / "macro_state_spine_us.parquet"
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None

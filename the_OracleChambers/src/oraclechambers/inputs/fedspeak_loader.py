"""
Helpers to work directly with FedSpeak / HKNSL leaves.
"""

from typing import Optional
import pandas as pd
from ..config import FEDSPEAK_DATA_DIR


def _maybe(name: str) -> Optional[pd.DataFrame]:
    path = FEDSPEAK_DATA_DIR / name
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def load_tone_leaves() -> Optional[pd.DataFrame]:
    return _maybe("fedspeak_tone_leaves.parquet")


def load_drift_leaves() -> Optional[pd.DataFrame]:
    return _maybe("fedspeak_drift_leaves.parquet")


def load_regime_leaves() -> Optional[pd.DataFrame]:
    return _maybe("fedspeak_regime_leaves.parquet")


def load_scenario_surface() -> Optional[pd.DataFrame]:
    return _maybe("fedspeak_scenario_surface.parquet")


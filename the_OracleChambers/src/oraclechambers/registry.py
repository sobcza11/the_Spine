"""
Central registry for OracleChambers.

Provides high-level accessors for upstream signals:
- the_Spine macro state (regimes, probabilities, macro leaves)
- FedSpeak / HKNSL communication leaves
- (later) markets data

All lenses and narrative engines should depend on this registry rather than
hardcoding paths.
"""

from dataclasses import dataclass
from typing import Optional
import pandas as pd

from .config import SPINE_DATA_DIR, FEDSPEAK_DATA_DIR


@dataclass
class SpineSources:
    macro_state_spine_us: Optional[pd.DataFrame] = None


@dataclass
class FedspeakSources:
    tone_leaves: Optional[pd.DataFrame] = None
    drift_leaves: Optional[pd.DataFrame] = None
    regime_leaves: Optional[pd.DataFrame] = None
    scenario_surface: Optional[pd.DataFrame] = None


@dataclass
class OracleContext:
    spine: SpineSources
    fedspeak: FedspeakSources


def _maybe_parquet(path) -> Optional[pd.DataFrame]:
    if hasattr(path, "exists") and path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    return None


def load_spine_sources() -> SpineSources:
    """Load core macro state from the_Spine (if present)."""
    macro_path = SPINE_DATA_DIR / "macro_state_spine_us.parquet"
    macro_df = _maybe_parquet(macro_path)
    return SpineSources(macro_state_spine_us=macro_df)


def load_fedspeak_sources() -> FedspeakSources:
    """Load FedSpeak/HKNSL leaf tables (if present)."""

    def maybe(name: str) -> Optional[pd.DataFrame]:
        p = FEDSPEAK_DATA_DIR / name
        return _maybe_parquet(p)

    return FedspeakSources(
        tone_leaves=maybe("fedspeak_tone_leaves.parquet"),
        drift_leaves=maybe("fedspeak_drift_leaves.parquet"),
        regime_leaves=maybe("fedspeak_regime_leaves.parquet"),
        scenario_surface=maybe("fedspeak_scenario_surface.parquet"),
    )


def build_context() -> OracleContext:
    """
    Primary entrypoint for lenses and narratives.

    Usage:
        from oraclechambers.registry import build_context
        ctx = build_context()
    """
    spine = load_spine_sources()
    fedspeak = load_fedspeak_sources()
    return OracleContext(spine=spine, fedspeak=fedspeak)

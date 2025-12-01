"""
Baseline narrative builder for FedSpeak leaves.

Reads combined_policy_leaf.parquet and emits short textual commentary
usable in briefs, decks, or dashboards.
"""

import pandas as pd
from pathlib import Path

from fed_speak.config import PROCESSED_DIR


def _classify_bias(policy_bias: float) -> str:
    if policy_bias > 0.1:
        return "tilt_hawkish"
    if policy_bias < -0.1:
        return "tilt_dovish"
    return "balanced"


def generate_latest_fedspeak_story() -> str:
    """
    Load the latest combined_policy_leaf row and produce a narrative summary.
    """
    df = pd.read_parquet(PROCESSED_DIR / "combined_policy_leaf.parquet")
    latest = df.sort_values("event_id").iloc[-1]

    bias_label = _classify_bias(latest["policy_bias"])

    narrative = (
        f"Fed BeigeBook tone currently reads as {bias_label}. "
        f"Inflation risk is {latest['inflation_risk']:.2f}, "
        f"while growth tone prints at {latest['growth_risk']:.2f}. "
        "This suggests a policy stance that is sensitive to price pressures "
        "while balancing growth considerations."
    )
    return narrative

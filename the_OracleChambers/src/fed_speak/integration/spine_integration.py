"""
Integration layer: FedSpeak → the_Spine MAIN_p.

Tranche 1: uses BeigeBook leaf as the first canonical input into p_Sentiment_US,
computing macro-friendly features like inflation_risk and growth_risk.
"""

import pandas as pd
from pathlib import Path

from fed_speak.config import PROCESSED_DIR


def load_beige_leaf() -> pd.DataFrame:
    return pd.read_parquet(PROCESSED_DIR / "BeigeBook" / "leaf.parquet")


def build_combined_policy_leaf() -> pd.DataFrame:
    """
    For Tranche 1, combined_policy_leaf is essentially the BeigeBook leaf,
    possibly extended later with Statement & Speeches.
    """
    beige_df = load_beige_leaf()

    # Simple mapping to macro-style risk indicators
    beige_df["inflation_risk"] = beige_df["price_pressure_score"]
    beige_df["growth_risk"] = beige_df["growth_tone_score"]
    beige_df["policy_bias"] = beige_df["hawkish_share"] - beige_df["dovish_share"]

    out_path = PROCESSED_DIR / "combined_policy_leaf.parquet"
    beige_df.to_parquet(out_path, index=False)
    return beige_df


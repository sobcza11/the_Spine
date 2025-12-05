from dataclasses import dataclass
from typing import Literal

import pandas as pd


FedRegimeName = Literal[
    "Disinflation_Recovery",
    "Soft_Landing_Risk_Drift",
    "Stagflation_Variant",
    "Illusory_Wealth_Regime",
    "Unknown",
]


@dataclass(frozen=True)
class FedSentimentConfig:
    """
    Mapping from Fed-only macro regimes to a single sentiment score in [-1, 1].

    Positive = supportive / easing-friendly
    Negative = restrictive / stagflation / policy-risk heavy
    """

    disinflation_recovery: float = 0.50
    soft_landing_risk_drift: float = -0.10
    stagflation_variant: float = -0.60
    illusory_wealth_regime: float = -0.30
    unknown: float = 0.00

    def as_mapping(self) -> dict[FedRegimeName, float]:
        return {
            "Disinflation_Recovery": self.disinflation_recovery,
            "Soft_Landing_Risk_Drift": self.soft_landing_risk_drift,
            "Stagflation_Variant": self.stagflation_variant,
            "Illusory_Wealth_Regime": self.illusory_wealth_regime,
            "Unknown": self.unknown,
        }


def compute_fed_sentiment(
    fed_regime_df: pd.DataFrame,
    config: FedSentimentConfig | None = None,
) -> pd.DataFrame:
    """
    Build the p_Sentiment_US (Fed component) leaf from a Fed-only macro regime panel.

    Expected input columns:
        - 'date' (datetime-like)
        - 'macro_state_spine_us_fed_only' (string regime label)

    Returns a new DataFrame with:
        - 'date'
        - 'fed_sentiment_score'      in [-1, 1]
        - 'fed_regime_label'         string regime name
    """
    if config is None:
        config = FedSentimentConfig()

    mapping = config.as_mapping().copy()

    df = fed_regime_df.copy()

    if "date" not in df.columns:
        raise ValueError("Expected column 'date' in fed_regime_df.")

    if "macro_state_spine_us_fed_only" not in df.columns:
        raise ValueError(
            "Expected column 'macro_state_spine_us_fed_only' in fed_regime_df."
        )

    df["fed_regime_label"] = df["macro_state_spine_us_fed_only"].astype(str)

    df["fed_sentiment_score"] = df["fed_regime_label"].map(mapping)

    if df["fed_sentiment_score"].isna().any():
        unknown_mask = df["fed_sentiment_score"].isna()
        if unknown_mask.any():
            # Fallback: treat any unexpected label as Unknown.
            df.loc[unknown_mask, "fed_regime_label"] = "Unknown"
            df.loc[unknown_mask, "fed_sentiment_score"] = mapping["Unknown"]

    # Keep only what the_Spine needs as the canonical Fed sentiment leaf
    out = df[["date", "fed_sentiment_score", "fed_regime_label"]].copy()
    out = out.sort_values("date").reset_index(drop=True)
    return out

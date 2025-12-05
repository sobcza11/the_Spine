"""
Generate FOMC Minutes sentiment & event-level leaf.

Inputs:
    data/processed/FOMC_Minutes/canonical_sentences.parquet

Outputs:
    data/processed/FOMC_Minutes/sentiment_scores.parquet
    data/processed/FOMC_Minutes/minutes_leaf.parquet
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


analyzer = SentimentIntensityAnalyzer()


def load_canonical(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Canonical sentences parquet not found at {path}")
    df = pd.read_parquet(path)
    required = {"event_id", "sentence_id", "sentence_text"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Canonical sentences missing required columns: {missing}")
    return df


def score_sentences(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Scoring FOMC Minutes sentences with VADER...")
    scores = []
    for _, row in df.iterrows():
        text = str(row["sentence_text"])
        vs = analyzer.polarity_scores(text)
        scores.append(
            {
                "event_id": row["event_id"],
                "sentence_id": row["sentence_id"],
                "vader_compound": vs["compound"],
                "vader_neg": vs["neg"],
                "vader_neu": vs["neu"],
                "vader_pos": vs["pos"],
            }
        )

    scores_df = pd.DataFrame(scores)

    # Simple tone classification from compound score
    def classify(comp: float) -> str:
        if comp > 0.1:
            return "dovish"
        if comp < -0.1:
            return "hawkish"
        return "neutral"

    scores_df["tone_label"] = scores_df["vader_compound"].apply(classify)
    return scores_df


def aggregate_to_event_leaf(scores_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate sentence-level VADER into an event-level leaf.

    For now we do a transparent, rule-based mapping:
        - inflation_risk: share of 'hawkish' minus share of 'dovish'
        - growth_risk: negative of inflation_risk (rough proxy, can be refined)
        - policy_bias: mean compound score
    """
    logger.info("Aggregating FOMC Minutes sentiment to event-level leaf...")

    def agg(group: pd.DataFrame) -> pd.Series:
        n = len(group)
        if n == 0:
            return pd.Series(
                {
                    "n_sentences": 0,
                    "share_hawkish": np.nan,
                    "share_dovish": np.nan,
                    "share_neutral": np.nan,
                    "mean_compound": np.nan,
                    "inflation_risk": np.nan,
                    "growth_risk": np.nan,
                    "policy_bias": np.nan,
                }
            )

        share_hawkish = (group["tone_label"] == "hawkish").mean()
        share_dovish = (group["tone_label"] == "dovish").mean()
        share_neutral = (group["tone_label"] == "neutral").mean()
        mean_compound = group["vader_compound"].mean()

        inflation_risk = share_hawkish - share_dovish
        growth_risk = -inflation_risk  # simple mirror; refine later
        policy_bias = mean_compound

        return pd.Series(
            {
                "n_sentences": n,
                "share_hawkish": share_hawkish,
                "share_dovish": share_dovish,
                "share_neutral": share_neutral,
                "mean_compound": mean_compound,
                "inflation_risk": inflation_risk,
                "growth_risk": growth_risk,
                "policy_bias": policy_bias,
            }
        )

    leaf_df = scores_df.groupby("event_id").apply(agg).reset_index()
    return leaf_df


def main() -> None:
    base_processed = Path("data/processed/FOMC_Minutes")
    canonical_path = base_processed / "canonical_sentences.parquet"
    scores_path = base_processed / "sentiment_scores.parquet"
    leaf_path = base_processed / "minutes_leaf.parquet"

    logger.info(f"Loading FOMC Minutes canonical sentences from {canonical_path}...")
    canonical_df = load_canonical(canonical_path)

    scores_df = score_sentences(canonical_df)
    base_processed.mkdir(parents=True, exist_ok=True)
    scores_df.to_parquet(scores_path, index=False)
    logger.info(f"Saved FOMC Minutes sentiment scores to {scores_path} with {len(scores_df)} rows.")

    leaf_df = aggregate_to_event_leaf(scores_df)
    leaf_df.to_parquet(leaf_path, index=False)
    logger.info(f"Saved FOMC Minutes leaf to {leaf_path} with {len(leaf_df)} rows.")


if __name__ == "__main__":
    main()

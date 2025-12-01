"""
FedSpeak leaves: aggregate sentence-level tone into document-level signals.

Tranche 1:
- BeigeBook: price/wage/growth tone
- FOMC_Statement: net hawkish shift (added later)
- Fed_Speeches: speaker drift (added later)
"""

import pandas as pd

from fed_speak.config import PROCESSED_DIR, LEAF_SUFFIX


def build_beige_leaf() -> pd.DataFrame:
    """
    Aggregate BeigeBook sentiment into district-level or national leaf.
    """
    category = "BeigeBook"
    df = pd.read_parquet(PROCESSED_DIR / category / "sentiment_scores.parquet")

    # Basic grouping: one row per event_id
    grouped = df.groupby("event_id").agg(
        price_pressure_score=("vader_compound", "mean"),
        # placeholder; later refine by filtering price-related sentences
        wage_pressure_score=("vader_compound", "mean"),
        growth_tone_score=("vader_compound", "mean"),
        hawkish_share=("tone_leaf", lambda x: (x == "hawkish").mean()),
        dovish_share=("tone_leaf", lambda x: (x == "dovish").mean()),
    )

    grouped = grouped.reset_index()
    out_dir = PROCESSED_DIR / category
    out_dir.mkdir(parents=True, exist_ok=True)
    grouped.to_parquet(out_dir / LEAF_SUFFIX, index=False)
    return grouped


def build_all_leaves():
    """
    Hook for Tranche 1 leaf generation.
    """
    build_beige_leaf()
    # Later:
    # build_statement_leaf()
    # build_speech_leaf()


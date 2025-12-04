"""
Baseline tone and sentiment scoring using VADER + rule-based tags.

Applies to:
- BeigeBook (Tranche 1)
- FOMC_Statement (when wired)
- Fed_Speeches (when wired)
"""

from typing import Dict
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from fed_speak.config import PROCESSED_DIR, TONE_SUFFIX

sia = SentimentIntensityAnalyzer()      


def _rule_based_tone(sentence: str, category: str) -> str:
    """
    Very simple placeholder for hawkish/dovish/neutral classification.
    You can refine lexicons by category.
    """
    text = sentence.lower()

    hawkish_terms = ["inflation", "overheating", "tightening", "rate hikes"]
    dovish_terms = ["slack", "unemployment", "accommodation", "support"]

    if any(term in text for term in hawkish_terms):
        return "hawkish"
    if any(term in text for term in dovish_terms):
        return "dovish"
    return "neutral"


def score_sentence(row: pd.Series) -> Dict[str, object]:
    """
    Apply VADER and rule-based tone to a single sentence row.
    """
    sentence = row["sentence_text"]
    category = row["category"]
    scores = sia.polarity_scores(sentence)
    tone_label = _rule_based_tone(sentence, category)
    return {
        "vader_neg": scores["neg"],
        "vader_neu": scores["neu"],
        "vader_pos": scores["pos"],
        "vader_compound": scores["compound"],
        "tone_leaf": tone_label,
    }


def build_tone_table(category: str) -> pd.DataFrame:
    """
    Reads canonical_sentences.parquet for a category,
    appends VADER + tone columns, and saves sentiment_scores.parquet.
    """
    in_path = PROCESSED_DIR / category / "canonical_sentences.parquet"
    out_dir = PROCESSED_DIR / category
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(in_path)
    tone_results = df.apply(score_sentence, axis=1, result_type="expand")
    df = pd.concat([df, tone_results], axis=1)

    df.to_parquet(out_dir / TONE_SUFFIX, index=False)
    return df


def build_all_tone():
    build_tone_table("BeigeBook")
    # Extend later:
    # build_tone_table("FOMC_Statement")
    # build_tone_table("Fed_Speeches")

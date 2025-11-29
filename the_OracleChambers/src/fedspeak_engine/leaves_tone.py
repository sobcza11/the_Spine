import pandas as pd

from .config import FED_CANONICAL_SENTENCES_PARQUET, FEDSPEAK_TONE_LEAVES_PARQUET
from .schemas import TONE_DTYPE, enforce_schema

HAWKISH_PATTERNS = [
    "inflation is elevated",
    "further tightening",
    "higher for longer",
    "strong labor market",
]
DOVISH_PATTERNS = [
    "accommodative",
    "support the economy",
    "gradual normalization",
    "lower for longer",
]


def score_sentence(sent: str) -> dict:
    s = sent.lower()
    hawk = sum(p in s for p in HAWKISH_PATTERNS)
    dov = sum(p in s for p in DOVISH_PATTERNS)
    total = hawk + dov

    if total == 0:
        return {
            "hawkish_score": 0.0,
            "dovish_score": 0.0,
            "neutral_score": 1.0,
            "uncertainty_score": 0.0,
            "tone_cluster": "neutral",
        }

    hawk_s = hawk / total
    dov_s = dov / total
    neutral_s = 0.0
    cluster = "hawkish" if hawk_s > dov_s else "dovish"

    return {
        "hawkish_score": float(hawk_s),
        "dovish_score": float(dov_s),
        "neutral_score": float(neutral_s),
        "uncertainty_score": 0.0,
        "tone_cluster": cluster,
    }


def build_tone_leaves() -> None:
    canon = pd.read_parquet(FED_CANONICAL_SENTENCES_PARQUET)
    records = []

    for _, row in canon.iterrows():
        scores = score_sentence(row["text_clean"])
        records.append(
            {
                "doc_id": row["doc_id"],
                "date": row["date"],
                "category": row["category"],
                "speaker": row["speaker"],
                "chair": row["chair"],
                "sentence_id": row["sentence_id"],
                **scores,
            }
        )

    df = pd.DataFrame(records)
    df = enforce_schema(df, TONE_DTYPE, datetime_cols=["date"])
    df.to_parquet(FEDSPEAK_TONE_LEAVES_PARQUET, index=False)
    print(f"Saved tone leaves to {FEDSPEAK_TONE_LEAVES_PARQUET}")


if __name__ == "__main__":
    build_tone_leaves()

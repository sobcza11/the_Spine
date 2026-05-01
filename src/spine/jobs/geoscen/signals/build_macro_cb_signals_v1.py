import os
import re
import pandas as pd

INPUT_PATH = "data/geoscen/cb/macro_cb_canonical_v1.parquet"
OUTPUT_PATH = "data/geoscen/signals/macro_cb_signals_v1.parquet"


HAWKISH_TERMS = [
    "inflation", "price stability", "tightening", "restrictive",
    "wage growth", "upside risks", "persistent inflation",
    "rate increase", "higher rates"
]

DOVISH_TERMS = [
    "easing", "rate cut", "lower rates", "weak growth",
    "downside risks", "recession", "slowing", "accommodative",
    "support the economy"
]

UNCERTAINTY_TERMS = [
    "uncertainty", "risks", "geopolitical", "financial stability",
    "volatility", "fragmentation", "stress"
]


def count_terms(text, terms):
    text = re.sub(r"[^a-zA-Z]+", " ", str(text).lower())
    return sum(text.count(term.lower()) for term in terms)


def run():
    df = pd.read_parquet(INPUT_PATH).copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["text"] = df["text"].fillna("").astype(str)

    df["hawkish_count"] = df["text"].apply(lambda x: count_terms(x, HAWKISH_TERMS))
    df["dovish_count"] = df["text"].apply(lambda x: count_terms(x, DOVISH_TERMS))
    df["uncertainty_count"] = df["text"].apply(lambda x: count_terms(x, UNCERTAINTY_TERMS))

    df["policy_tone_score"] = df["hawkish_count"] - df["dovish_count"]
    df["uncertainty_score"] = df["uncertainty_count"]

    df["text_word_count"] = df["text"].str.split().str.len()

    df["policy_tone_per_1k_words"] = (
        df["policy_tone_score"] / df["text_word_count"].replace(0, pd.NA) * 1000
    )

    df["uncertainty_per_1k_words"] = (
        df["uncertainty_score"] / df["text_word_count"].replace(0, pd.NA) * 1000
    )

    keep_cols = [
        "bank",
        "bank_code",
        "currency",
        "date",
        "document_type",
        "title",
        "url",
        "text_chars",
        "text_word_count",
        "hawkish_count",
        "dovish_count",
        "uncertainty_count",
        "policy_tone_score",
        "uncertainty_score",
        "policy_tone_per_1k_words",
        "uncertainty_per_1k_words",
    ]

    out = df[keep_cols].sort_values(["bank_code", "date", "document_type"]).reset_index(drop=True)

    out = out.sort_values(
        ["bank_code", "date", "document_type", "policy_tone_score", "uncertainty_score", "text_chars"],
        ascending=[True, True, True, False, False, False],
    )

    out = out.drop_duplicates(
        subset=["bank_code", "date", "document_type"],
        keep="first",
    ).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("macro_cb signals rows:", len(out))


if __name__ == "__main__":
    run()


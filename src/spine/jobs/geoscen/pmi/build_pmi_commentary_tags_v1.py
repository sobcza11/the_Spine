import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    CANONICAL_OUTPUT,
    TAGS_OUTPUT,
)

TAG_DICTIONARY = {
    "labor": [
        "hiring", "employment", "labor", "labour", "workers", "staffing",
        "wages", "headcount", "layoff", "shortage"
    ],
    "inflation": [
        "prices", "price", "inflation", "cost", "costs", "fuel",
        "input costs", "higher costs", "margin pressure"
    ],
    "credit": [
        "credit", "financing", "rates", "interest", "borrowing",
        "capital", "liquidity"
    ],
    "demand": [
        "demand", "orders", "new orders", "sales", "customer",
        "backlog", "resilient", "softening"
    ],
    "supply_chain": [
        "supply chain", "shipping", "freight", "logistics", "delivery",
        "supplier", "inventory", "blockage", "shortages"
    ],
    "geopolitics": [
        "geopolitical", "tariff", "iran", "china", "war", "conflict",
        "sanctions", "trade disruption"
    ],
}

POSITIVE_WORDS = [
    "strong", "strength", "resilient", "improved", "improving",
    "growth", "expansion", "higher demand", "increase"
]

NEGATIVE_WORDS = [
    "weak", "weakness", "decline", "declining", "soft", "softening",
    "pressure", "shortage", "disruption", "blockage", "higher costs",
    "slowing", "contraction"
]


def contains_any(text: str, terms: list[str]) -> int:
    text = str(text).lower()
    return int(any(term in text for term in terms))


def score_direction(text: str) -> str:
    text = str(text).lower()

    pos = sum(term in text for term in POSITIVE_WORDS)
    neg = sum(term in text for term in NEGATIVE_WORDS)

    if pos > neg:
        return "+"
    if neg > pos:
        return "-"
    return "neutral"


def build_pmi_commentary_tags_v1() -> pd.DataFrame:
    df = pd.read_parquet(CANONICAL_OUTPUT).copy()

    for tag, terms in TAG_DICTIONARY.items():
        df[f"tag_{tag}"] = df["commentary_text"].apply(lambda x: contains_any(x, terms))

    df["direction"] = df["commentary_text"].apply(score_direction)

    tag_cols = [f"tag_{tag}" for tag in TAG_DICTIONARY]
    df["tag_count"] = df[tag_cols].sum(axis=1)
    df["confidence"] = df["tag_count"].clip(upper=3) / 3

    TAGS_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(TAGS_OUTPUT, index=False)

    print(f"[OK] Wrote PMI commentary tags ({len(df)} rows)")
    print(f"[PATH] {TAGS_OUTPUT}")

    return df


if __name__ == "__main__":
    build_pmi_commentary_tags_v1()


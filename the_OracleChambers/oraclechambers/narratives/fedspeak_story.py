from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd


@dataclass
class TopicSummary:
    topic_id: int
    top_terms: List[str]
    avg_prob: float


def _load_latest_policy_leaf(path: Path) -> Optional[pd.Series]:
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty:
        return None
    # assume there is a date or event_id we can sort on; adjust as needed
    if "event_date" in df.columns:
        df = df.sort_values("event_date")
    return df.iloc[-1]


def _load_topics(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty or "topic_id" not in df.columns or "topic_prob" not in df.columns:
        return None
    return df


def _summarize_topics(
    topics_df: pd.DataFrame,
    max_topics: int = 3,
    max_terms_per_topic: int = 4,
) -> List[TopicSummary]:
    """
    Simple topic summarizer:
    - ranks topics by average topic_prob
    - extracts top terms directly from sentence_text (bag-of-words fallback)
    """
    topics_df = topics_df.dropna(subset=["topic_id"]).copy()
    topics_df["topic_id"] = topics_df["topic_id"].astype(int)

    avg_prob = topics_df.groupby("topic_id")["topic_prob"].mean().reset_index()
    top_topics = avg_prob.sort_values("topic_prob", ascending=False).head(max_topics)

    summaries: List[TopicSummary] = []
    for _, row in top_topics.iterrows():
        topic_id = int(row["topic_id"])
        subset = topics_df[topics_df["topic_id"] == topic_id]["sentence_text"].astype(str)
        # crude term extraction: most common words across sentences
        all_words: List[str] = []
        for text in subset:
            all_words.extend([w.lower() for w in text.split() if w.isalpha()])

        if not all_words:
            summaries.append(TopicSummary(topic_id=topic_id, top_terms=[], avg_prob=row["topic_prob"]))
            continue

        counts = {}
        for w in all_words:
            counts[w] = counts.get(w, 0) + 1

        # remove ultra-common boilerplate terms
        blacklist = {"the", "and", "of", "to", "in", "for", "a", "on", "with", "is"}
        filtered = [(w, c) for w, c in counts.items() if w not in blacklist]
        filtered.sort(key=lambda x: x[1], reverse=True)
        top_terms = [w for w, _ in filtered[:max_terms_per_topic]]

        summaries.append(
            TopicSummary(
                topic_id=topic_id,
                top_terms=top_terms,
                avg_prob=float(row["topic_prob"]),
            )
        )

    return summaries


def _format_topic_bullets(summaries: List[TopicSummary]) -> List[str]:
    bullets = []
    for s in summaries:
        if s.top_terms:
            label = ", ".join(s.top_terms)
        else:
            label = "no clear keyword cluster"
        bullets.append(f"• Topic {s.topic_id}: {label}")
    return bullets


def generate_latest_fedspeak_story(
    combined_leaf_path: Path = Path("data/processed/FedSpeak/combined_policy_leaf.parquet"),
    beige_topics_path: Path = Path("data/processed/BeigeBook/beige_topics.parquet"),
) -> str:
    """
    High-level narrative generator:
    - reads latest multi-channel Fed policy leaf
    - augments with BeigeBook topics summary (HKNSL-style)
    """

    latest_leaf = _load_latest_policy_leaf(combined_leaf_path)
    if latest_leaf is None:
        return "FedSpeak: no policy leaf available to generate a narrative."

    inflation_risk = latest_leaf.get("inflation_risk", 0.0)
    growth_risk = latest_leaf.get("growth_risk", 0.0)
    policy_bias = latest_leaf.get("policy_bias", 0.0)

    # Headline tone (you can refine thresholds)
    if policy_bias > 0.2:
        headline = "Fed tone remains biased toward tightening."
    elif policy_bias < -0.2:
        headline = "Fed tone leans toward easing support."
    else:
        headline = "Fed tone appears broadly balanced around current policy settings."

    bullets: List[str] = []
    bullets.append(f"• Inflation risk signal: {inflation_risk:+.2f}")
    bullets.append(f"• Growth risk signal: {growth_risk:+.2f}")
    bullets.append(f"• Net policy bias score: {policy_bias:+.2f}")

    # BeigeBook topic overlay (Tranche 2)
    topics_df = _load_topics(beige_topics_path)
    if topics_df is not None:
        topic_summaries = _summarize_topics(topics_df)
        if topic_summaries:
            bullets.append("")
            bullets.append("BeigeBook topic drift (HKNSL-style):")
            bullets.extend(_format_topic_bullets(topic_summaries))

    story = headline + "\n" + "\n".join(bullets)
    return story


if __name__ == "__main__":
    print(generate_latest_fedspeak_story())



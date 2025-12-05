# minutes_parser.py

from typing import List, Dict
import re

from .minutes_schema import ParagraphSignal
from .minutes_lexicon import (
    INFLATION_PHRASES,
    GROWTH_PHRASES,
    UNCERTAINTY_PHRASES,
    DISSENT_PHRASES,
    TOPIC_TAGS,
)


def split_into_paragraphs(text: str) -> List[str]:
    """
    Simple paragraph splitter based on blank lines.
    Assumes Minutes text already normalized to Unix newlines.
    """
    raw_paragraphs = re.split(r"\n\s*\n", text.strip())
    paragraphs = [p.strip() for p in raw_paragraphs if p.strip()]
    return paragraphs


def _score_text_with_lexicon(text: str, lexicon: Dict[str, float]) -> float:
    """
    Score text by summing weights of matching phrases, then compressing to [-1, 1].
    """
    t = text.lower()
    score = 0.0
    for phrase, weight in lexicon.items():
        if phrase in t:
            score += weight

    # Compress using tanh-like scaling (simple heuristic).
    # This keeps scores in a manageable range.
    if score == 0:
        return 0.0
    return max(-1.0, min(1.0, score / 2.0))


def _infer_topic_tags(text: str) -> List[str]:
    t = text.lower()
    tags: List[str] = []
    for topic, keywords in TOPIC_TAGS.items():
        if any(k in t for k in keywords):
            tags.append(topic)
    return tags


def analyze_paragraphs(text: str) -> List[ParagraphSignal]:
    """
    Main function: split Minutes into paragraphs and produce ParagraphSignal objects.
    """
    paragraphs = split_into_paragraphs(text)
    signals: List[ParagraphSignal] = []

    for idx, p in enumerate(paragraphs):
        infl = _score_text_with_lexicon(p, INFLATION_PHRASES)
        gr = _score_text_with_lexicon(p, GROWTH_PHRASES)
        unc = _score_text_with_lexicon(p, UNCERTAINTY_PHRASES)
        dis = _score_text_with_lexicon(p, DISSENT_PHRASES)
        tags = _infer_topic_tags(p)

        signals.append(
            ParagraphSignal(
                index=idx,
                text=p,
                inflation_risk=infl,
                growth_risk=gr,
                uncertainty=min(max(unc, 0.0), 1.0),  # compress to [0, 1]
                dissent_score=min(max(dis, 0.0), 1.0),
                topic_tags=tags,
            )
        )

    return signals


# minutes_lexicon.py

from typing import Dict, List


def _lower_keys(d: Dict[str, float]) -> Dict[str, float]:
    return {k.lower(): v for k, v in d.items()}


# Phrases that signal more inflation concern (positive) or less (negative).
INFLATION_PHRASES: Dict[str, float] = _lower_keys(
    {
        "inflation remains elevated": 0.8,
        "upside risks to inflation": 0.7,
        "inflation pressures": 0.6,
        "inflation expectations": 0.4,
        "disinflationary pressures": -0.7,
        "inflation has eased": -0.5,
        "inflation moving down": -0.4,
    }
)

# Phrases that signal growth risk (negative = weaker growth).
GROWTH_PHRASES: Dict[str, float] = _lower_keys(
    {
        "softening in economic activity": -0.6,
        "weakening labor market": -0.7,
        "slower growth": -0.5,
        "sluggish demand": -0.5,
        "robust economic activity": 0.6,
        "solid job gains": 0.5,
        "strong consumer spending": 0.5,
    }
)

# Phrases that signal uncertainty / disagreement.
UNCERTAINTY_PHRASES: Dict[str, float] = _lower_keys(
    {
        "uncertain": 0.4,
        "highly uncertain": 0.7,
        "uncertainty": 0.5,
        "range of views": 0.6,
        "mixed signals": 0.6,
        "substantial uncertainty": 0.8,
    }
)

# Phrases that hint at dissent or disagreement.
DISSENT_PHRASES: Dict[str, float] = _lower_keys(
    {
        "some participants": 0.3,
        "a few participants": 0.4,
        "several participants": 0.5,
        "dissented": 0.9,
        "did not support": 0.7,
        "preferred a higher target range": 0.8,
        "preferred a lower target range": 0.8,
    }
)

# Optional: topic tags to classify paragraphs (growth, inflation, labor, financial conditions, etc.)
TOPIC_TAGS: Dict[str, List[str]] = {
    "inflation": ["inflation", "prices", "price pressures", "wage pressures"],
    "labor_market": ["labor market", "employment", "unemployment", "job gains"],
    "growth": ["economic activity", "output", "growth", "demand"],
    "financial_conditions": ["financial conditions", "credit", "lending", "markets"],
}


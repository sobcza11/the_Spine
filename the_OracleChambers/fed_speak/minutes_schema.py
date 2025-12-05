# minutes_schema.py

from dataclasses import dataclass, asdict
from datetime import date
from typing import Dict, List, Optional, Any


@dataclass
class ParagraphSignal:
    """Signal-level representation of a single paragraph in the Minutes."""

    index: int
    text: str
    inflation_risk: float  # [-1, 1]
    growth_risk: float     # [-1, 1]
    uncertainty: float     # [0, 1]
    dissent_score: float   # [0, 1]
    topic_tags: List[str]


@dataclass
class MinutesSignal:
    """
    Document-level signal extracted from an FOMC Minutes document.
    Designed to be easily serialized into JSON and ingested into the_Spine.
    """

    meeting_id: str
    meeting_date: date
    source: str  # e.g., "FOMC Minutes"
    inflation_risk: float      # [-1, 1]
    growth_risk: float         # [-1, 1]
    uncertainty: float         # [0, 1]
    dissent_score: float       # [0, 1]
    stance_coherence: float    # [0, 1]  # higher = more coherent / aligned
    paragraph_signals: List[ParagraphSignal]
    meta: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a plain dictionary for JSON serialization."""
        return {
            "meeting_id": self.meeting_id,
            "meeting_date": self.meeting_date.isoformat(),
            "source": self.source,
            "inflation_risk": self.inflation_risk,
            "growth_risk": self.growth_risk,
            "uncertainty": self.uncertainty,
            "dissent_score": self.dissent_score,
            "stance_coherence": self.stance_coherence,
            "paragraph_signals": [asdict(p) for p in self.paragraph_signals],
            "meta": self.meta,
        }


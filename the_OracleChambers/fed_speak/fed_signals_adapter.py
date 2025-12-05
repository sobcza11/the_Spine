# fed_speak/fed_signals_adapter.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Any
import json


@dataclass
class FedMinutesFeatures:
    meeting_id: str
    meeting_date: date
    fed_minutes_inflation_risk: float
    fed_minutes_growth_risk: float
    fed_minutes_uncertainty: float
    fed_minutes_dissent: float
    fed_minutes_stance_coherence: float

    def as_feature_dict(self) -> Dict[str, float]:
        return {
            "fed_minutes_inflation_risk": self.fed_minutes_inflation_risk,
            "fed_minutes_growth_risk": self.fed_minutes_growth_risk,
            "fed_minutes_uncertainty": self.fed_minutes_uncertainty,
            "fed_minutes_dissent": self.fed_minutes_dissent,
            "fed_minutes_stance_coherence": self.fed_minutes_stance_coherence,
        }


def _parse_iso_date(d: str) -> date:
    y, m, day = d.split("-")
    return date(int(y), int(m), int(day))


def minutes_signal_json_to_features(path: Path) -> FedMinutesFeatures:
    """
    Load a single Minutes signal JSON (as written by run_minutes_to_signals.py)
    and convert it to FedMinutesFeatures for the_Spine.
    """
    with path.open("r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)

    meeting_id = data["meeting_id"]
    meeting_date = _parse_iso_date(data["meeting_date"])

    return FedMinutesFeatures(
        meeting_id=meeting_id,
        meeting_date=meeting_date,
        fed_minutes_inflation_risk=float(data.get("inflation_risk", 0.0)),
        fed_minutes_growth_risk=float(data.get("growth_risk", 0.0)),
        fed_minutes_uncertainty=float(data.get("uncertainty", 0.0)),
        fed_minutes_dissent=float(data.get("dissent_score", 0.0)),
        fed_minutes_stance_coherence=float(data.get("stance_coherence", 0.0)),
    )


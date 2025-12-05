# minutes_engine.py

from datetime import date
from typing import Dict, Any, Optional, List

import math

from .minutes_schema import MinutesSignal, ParagraphSignal
from .minutes_parser import analyze_paragraphs


def _average(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _stance_coherence(paragraph_signals: List[ParagraphSignal]) -> float:
    """
    Simple coherence metric:
    - Lower variance in inflation/growth views → higher coherence.
    - Range-normalized to [0, 1].
    """
    if not paragraph_signals:
        return 0.0

    infl_vals = [p.inflation_risk for p in paragraph_signals]
    gr_vals = [p.growth_risk for p in paragraph_signals]

    def var(xs: List[float]) -> float:
        if len(xs) <= 1:
            return 0.0
        m = sum(xs) / len(xs)
        return sum((x - m) ** 2 for x in xs) / (len(xs) - 1)

    infl_var = var(infl_vals)
    gr_var = var(gr_vals)

    # Higher variance → lower coherence.
    raw = infl_var + gr_var
    # Map to [0, 1] via 1 / (1 + k * raw). k chosen ad hoc for scaling.
    k = 2.0
    coherence = 1.0 / (1.0 + k * raw)
    return max(0.0, min(1.0, coherence))


def extract_minutes_signal(
    text: str,
    meeting_id: str,
    meeting_date: date,
    meta: Optional[Dict[str, Any]] = None,
) -> MinutesSignal:
    """
    Main public entrypoint for the FOMC Minutes Engine.

    Inputs:
        text        - raw Minutes text (single string, normalized newlines)
        meeting_id  - your internal ID (e.g., "FOMC_2024_09")
        meeting_date- official meeting date
        meta        - any additional metadata (url, source, version, etc.)

    Output:
        MinutesSignal, ready to be serialized into JSON and ingested into the_Spine.
    """
    if meta is None:
        meta = {}

    paragraph_signals = analyze_paragraphs(text)

    infl = _average([p.inflation_risk for p in paragraph_signals])
    gr = _average([p.growth_risk for p in paragraph_signals])
    unc = _average([p.uncertainty for p in paragraph_signals])
    dis = _average([p.dissent_score for p in paragraph_signals])
    coh = _stance_coherence(paragraph_signals)

    signal = MinutesSignal(
        meeting_id=meeting_id,
        meeting_date=meeting_date,
        source="FOMC Minutes",
        inflation_risk=infl,
        growth_risk=gr,
        uncertainty=unc,
        dissent_score=dis,
        stance_coherence=coh,
        paragraph_signals=paragraph_signals,
        meta=meta,
    )

    return signal


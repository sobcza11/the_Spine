from __future__ import annotations


def infer_regime_bucket(score: float) -> str:
    if score <= 30:
        return "Stable"
    if score <= 55:
        return "Tension"
    if score <= 75:
        return "Crisis Risk"
    return "Contagion"


def explain_regime(score: float) -> str:
    bucket = infer_regime_bucket(score)
    if bucket == "Stable":
        return "Stress remains contained."
    if bucket == "Tension":
        return "Stress is elevated but not yet acute."
    if bucket == "Crisis Risk":
        return "Stress is high & broadening across macro channels."
    return "Stress is acute with contagion risk elevated."


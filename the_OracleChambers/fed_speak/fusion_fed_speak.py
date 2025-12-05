from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Dict, Tuple
import math


# Regime names aligned with the_Spine TRANCHE 1 macro states
REGIME_NAMES = [
    "Disinflation_Recovery",
    "Soft_Landing_Risk_Drift",
    "Stagflation_Variant",
    "Illusory_Wealth_Regime",
    "Unknown",
]


@dataclass
class FedFusionOutput:
    """
    Container for FedSpeak-implied regime weights.

    weights: dict of regime_name -> probability (sums to 1.0)
    """
    weights: Dict[str, float]

    def get(self, regime: str) -> float:
        return float(self.weights.get(regime, 0.0))


def _softmax(scores: Dict[str, float]) -> Dict[str, float]:
    """Simple numerically-stable softmax over a dict of scores."""
    if not scores:
        return {r: 1.0 / len(REGIME_NAMES) for r in REGIME_NAMES}

    max_score = max(scores.values())
    exps = {k: math.exp(v - max_score) for k, v in scores.items()}
    total = sum(exps.values()) or 1.0
    return {k: v / total for k, v in exps.items()}


def compute_fed_composites(row) -> Dict[str, float]:
    """
    Compute raw composite Fed signals from a row (Series or dict).

    Returns a dict with:
        - inflation_pressure
        - growth_concern
        - policy_uncertainty
        - policy_coherence
        - policy_bias
    """

    def g(name: str, default: float = 0.0) -> float:
        """
        Safe getter: handles missing keys, None, and NaN by returning `default`.
        """
        val = row.get(name, default)

        if val is None:
            return default

        try:
            v = float(val)
        except Exception:
            return default

        if math.isnan(v):
            return default

        return v

    # Minutes
    infl_minutes = g("fed_minutes_inflation_risk")
    growth_minutes = g("fed_minutes_growth_risk")
    unc_minutes = g("fed_minutes_uncertainty")
    stance_coherence = g("fed_minutes_stance_coherence")

    # Statements
    infl_stmt = g("fed_stmt_inflation_focus")
    unc_stmt = g("fed_stmt_uncertainty")
    policy_bias = g("fed_stmt_policy_bias")

    # Beige
    beige_price = g("beige_price_tone")    # [-1, 1]
    beige_growth = g("beige_growth_tone")  # [-1, 1]
    beige_unc = g("beige_uncertainty")

    # Composites
    inflation_pressure = (
        0.5 * infl_minutes +
        0.3 * infl_stmt +
        0.2 * max(beige_price, 0.0)
    )

    # beige_growth [-1, 1] -> softness [0, 1], higher = weaker growth
    beige_softness = (1.0 - beige_growth) / 2.0
    growth_concern = (
        0.6 * (-growth_minutes) +
        0.4 * beige_softness
    )

    policy_uncertainty = (
        0.5 * unc_minutes +
        0.3 * unc_stmt +
        0.2 * beige_unc
    )

    return {
        "inflation_pressure": inflation_pressure,
        "growth_concern": growth_concern,
        "policy_uncertainty": policy_uncertainty,
        "policy_coherence": stance_coherence,
        "policy_bias": policy_bias,
    }


def build_quantile_thresholds(
    df,
    q_low: float = 0.25,
    q_high: float = 0.75,
) -> Dict[str, Tuple[float, float]]:
    """
    Build data-driven low/high thresholds for each composite signal,
    using quantiles from the actual Fed block.

    Returns a dict:
        {
          "inflation_pressure": (low, high),
          "growth_concern": (low, high),
          "policy_uncertainty": (low, high),
        }
    """
    import pandas as pd

    comps = df.apply(lambda r: pd.Series(compute_fed_composites(r)), axis=1)

    thresholds: Dict[str, Tuple[float, float]] = {}
    for name in ("inflation_pressure", "growth_concern", "policy_uncertainty"):
        series = comps[name].dropna()
        if len(series) == 0:
            thresholds[name] = (0.0, 0.0)
        else:
            low = float(series.quantile(q_low))
            high = float(series.quantile(q_high))
            thresholds[name] = (low, high)

    return thresholds


def compute_fed_regime_weights(
    row: Mapping[str, float],
    thresholds: Dict[str, Tuple[float, float]] | None = None,
) -> FedFusionOutput:
    """
    Given a mapping (e.g. pandas Series) with FedSpeak features, compute
    normalized regime weights implied by Fed language.

    Expected (optional) keys; missing/None treated as 0.0:

        fed_minutes_inflation_risk
        fed_minutes_growth_risk
        fed_minutes_uncertainty
        fed_minutes_dissent
        fed_minutes_stance_coherence

        fed_stmt_policy_bias
        fed_stmt_inflation_focus
        fed_stmt_growth_focus
        fed_stmt_uncertainty

        beige_growth_tone
        beige_price_tone
        beige_wage_tone
        beige_uncertainty

    thresholds (optional):
        dict returned by build_quantile_thresholds(df), providing
        low/high cutoffs for:
          - inflation_pressure
          - growth_concern
          - policy_uncertainty
    """

    # ---------- 1) Composite signals ---------------------------------------
    comps = compute_fed_composites(row)
    inflation_pressure = comps["inflation_pressure"]
    growth_concern = comps["growth_concern"]
    policy_uncertainty = comps["policy_uncertainty"]
    policy_coherence = comps["policy_coherence"]
    policy_bias = comps["policy_bias"]

    # ---------- 2) Bucket signals into low / mid / high --------------------

    def bucket(value: float, low: float, high: float) -> str:
        if value <= low:
            return "low"
        if value >= high:
            return "high"
        return "mid"

    if thresholds is None:
        infl_low, infl_high = 0.0025, 0.0060
        growth_low, growth_high = 0.20, 0.40
        unc_low, unc_high = 0.005, 0.015
    else:
        infl_low, infl_high = thresholds["inflation_pressure"]
        growth_low, growth_high = thresholds["growth_concern"]
        unc_low, unc_high = thresholds["policy_uncertainty"]

    infl_bucket = bucket(inflation_pressure, low=infl_low, high=infl_high)
    growth_bucket = bucket(growth_concern, low=growth_low, high=growth_high)
    unc_bucket = bucket(policy_uncertainty, low=unc_low, high=unc_high)
    # invert coherence so coh_bucket describes *incoherence*
    coh_bucket = bucket(1.0 - policy_coherence, low=0.02, high=0.08)

    # ---------- 3) Regime scoring logic ------------------------------------

    scores: Dict[str, float] = {name: 0.0 for name in REGIME_NAMES}

    # If everything is low, lean Unknown a bit
    if all(b == "low" for b in (infl_bucket, growth_bucket, unc_bucket)):
        scores["Unknown"] += 1.0

    # Disinflation Recovery:
    # low inflation, low growth concern, low uncertainty, coherent
    if infl_bucket == "low" and growth_bucket == "low":
        scores["Disinflation_Recovery"] += 1.0
        if unc_bucket == "low":
            scores["Disinflation_Recovery"] += 0.5
        if coh_bucket == "low":  # very coherent
            scores["Disinflation_Recovery"] += 0.5

    # Soft-Landing Risk Drift:
    # mid inflation, mild-to-mid growth concern, elevated uncertainty
    if infl_bucket == "mid" and growth_bucket in ("low", "mid"):
        scores["Soft_Landing_Risk_Drift"] += 1.0
        if unc_bucket in ("mid", "high"):
            scores["Soft_Landing_Risk_Drift"] += 0.5

    # Stagflation Variant:
    # high inflation + high growth concern + some uncertainty
    if infl_bucket == "high" and growth_bucket == "high":
        scores["Stagflation_Variant"] += 1.5
        if unc_bucket in ("mid", "high"):
            scores["Stagflation_Variant"] += 0.5

    # Illusory Wealth Regime:
    # elevated inflation pressure + dovish bias + coherent messaging
    if infl_bucket in ("mid", "high") and policy_bias < 0.0:
        scores["Illusory_Wealth_Regime"] += 1.0
        if coh_bucket == "low":  # highly coherent but mis-calibrated
            scores["Illusory_Wealth_Regime"] += 0.5
        if growth_bucket == "low":  # Fed sounds relaxed on growth
            scores["Illusory_Wealth_Regime"] += 0.5

    # If all scores are negligible, fall back to uniform
    if all(abs(v) < 1e-6 for v in scores.values()):
        weights = {r: 1.0 / len(REGIME_NAMES) for r in REGIME_NAMES}
    else:
        weights = _softmax(scores)

    return FedFusionOutput(weights=weights)




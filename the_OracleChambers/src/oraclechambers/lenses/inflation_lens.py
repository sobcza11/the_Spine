"""
Inflation Lens v0

Takes the latest macro_state_spine_us row (from the_Spine) and produces a
compact interpretation of the inflation/energy situation.
"""

from typing import Dict, Optional
import pandas as pd

from ..utils.time_windows import latest_row


def interpret_inflation(latest: Optional[pd.Series]) -> Dict[str, str]:
    """
    Convert the latest macro_state_spine_us row into a few narrative atoms.

    Returns a dict with:
        - headline
        - inflation_state
        - energy_state
        - risk_flags
    """
    if latest is None:
        return {
            "headline": "Inflation lens: no macro state data available.",
            "inflation_state": "Unknown – macro_state_spine_us is missing or empty.",
            "energy_state": "Unknown – energy information not available.",
            "risk_flags": "No inflation risk flags can be computed yet.",
        }

    # Safe access helpers
    def g(col, default=None):
        return latest[col] if col in latest.index else default

    regime = g("regime_label", "Unknown regime")
    infl_pulse = g("inflation_pulse", None)
    energy_spread = g("energy_spread_score", None)
    core_gap = g("core_vs_headline_gap", None)

    # Inflation state text
    if infl_pulse is None:
        inflation_state = "Inflation dynamics are not quantified yet, but the current regime is reported as: " + str(
            regime
        )
    elif infl_pulse > 0.5:
        inflation_state = (
            f"Inflation pressures remain elevated, consistent with a positive inflation pulse of {infl_pulse:.2f}."
        )
    elif infl_pulse < -0.5:
        inflation_state = (
            f"Inflation pressures appear to be easing, with a negative inflation pulse of {infl_pulse:.2f}."
        )
    else:
        inflation_state = (
            f"Inflation pressures look relatively balanced, with a modest inflation pulse of {infl_pulse:.2f}."
        )

    # Energy state text
    if energy_spread is None:
        energy_state = "Energy spreads are not yet modeled in the current snapshot."
    elif energy_spread > 0.5:
        energy_state = (
            f"Energy spreads suggest upside risk to inflation (energy_spread_score={energy_spread:.2f})."
        )
    elif energy_spread < -0.5:
        energy_state = (
            f"Energy spreads point to easing price pressure from energy (energy_spread_score={energy_spread:.2f})."
        )
    else:
        energy_state = (
            f"Energy spreads look relatively neutral (energy_spread_score={energy_spread:.2f})."
        )

    # Risk flags
    risk_bits = []
    if infl_pulse is not None and infl_pulse > 0.5:
        risk_bits.append("Persistent inflation upside")
    if energy_spread is not None and energy_spread > 0.5:
        risk_bits.append("Energy-driven price risk")
    if core_gap is not None and core_gap > 0.4:
        risk_bits.append("Core remains sticky vs. headline")

    if not risk_bits:
        risk_flags = "No acute inflation-specific risk flags triggered in this snapshot."
    else:
        risk_flags = "; ".join(risk_bits) + "."

    headline = f"Inflation lens view anchored in regime: {regime}"

    return {
        "headline": headline,
        "inflation_state": inflation_state,
        "energy_state": energy_state,
        "risk_flags": risk_flags,
    }

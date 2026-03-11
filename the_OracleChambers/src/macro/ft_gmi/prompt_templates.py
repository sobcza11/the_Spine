REGIME_TEXT = {
    "Stable": "Macro stress remains contained.",
    "Tension": "Macro stress is elevated & worth close monitoring.",
    "Crisis Risk": "Macro stress is high & regime deterioration risk is rising.",
    "Contagion": "Systemic stress appears acute with contagion risk elevated.",
}

DRIVER_TEXT = {
    "rates_stress": "rates pressure",
    "fx_stress": "FX pressure",
    "energy_stress": "energy shock pressure",
    "equity_stress": "equity risk pressure",
    "credit_stress": "credit stress pressure",
}

VALIDATOR_TEXT = {
    "OK": "Validator status is ok with no material structural issues detected.",
    "WARN": "Validator status is warn, indicating elevated signal interpretation caution.",
    "FAIL": "Validator status is fail, meaning this signal should not be relied on for narrative use.",
}

def narrative_template(score, regime, drivers):

    return f"""
Macro stress is elevated & worth monitoring.

FT-GMI score: {score:.1f}
Regime: {regime}

Dominant drivers:
{drivers}
"""


from __future__ import annotations

from typing import Any

from macro.ft_gmi.ft_gmi_reader import read_ft_gmi_latest
from macro.ft_gmi.prompt_templates import DRIVER_TEXT, REGIME_TEXT, VALIDATOR_TEXT
from macro.ft_gmi.regime_explainer import infer_regime_bucket

from .ft_gmi_reader import load_ft_gmi
from .feature_engineering import build_features
from .regime_explainer import regime_bucket
from .prompt_templates import narrative_template




COMPONENTS = [
    "rates_stress",
    "fx_stress",
    "energy_stress",
    "equity_stress",
    "credit_stress",
]

FWD_COMPONENTS = [
    "rates_stress_fwd45d",
    "fx_stress_fwd45d",
    "energy_stress_fwd45d",
    "equity_stress_fwd45d",
    "credit_stress_fwd45d",
]

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _top_driver(row: dict) -> tuple[str, float]:
    vals = {c: _safe_float(row.get(c, 0.0)) for c in COMPONENTS}
    k = max(vals, key=vals.get)
    return k, vals[k]


def _top_fwd_driver(row: dict) -> tuple[str, float]:
    vals = {c: _safe_float(row.get(c, 0.0)) for c in FWD_COMPONENTS if c in row}
    if not vals:
        return "none", 0.0
    k = max(vals, key=vals.get)
    return k, vals[k]


def _validator_summary(row: dict) -> str:
    status = str(row.get("validator_status", "OK")).upper()
    warning_count = int(_safe_float(row.get("validator_warning_count", 0)))
    base = VALIDATOR_TEXT.get(status, f"Validator status is {status.lower()}.")
    if warning_count <= 0:
        return base
    return f"{base} Active warning count: {warning_count}."


def build_macro_narrative() -> str:
    row = read_ft_gmi_latest()

    score = _safe_float(row.get("ft_gmi_score", 0.0))
    bucket = str(row.get("regime_bucket") or infer_regime_bucket(score))

    rates = _safe_float(row.get("rates_stress", 0.0))
    fx = _safe_float(row.get("fx_stress", 0.0))
    energy = _safe_float(row.get("energy_stress", 0.0))
    equity = _safe_float(row.get("equity_stress", 0.0))
    credit = _safe_float(row.get("credit_stress", 0.0))
    dispersion = _safe_float(row.get("dispersion_score", 0.0))

    top_driver, top_driver_score = _top_driver(row)
    top_fwd_driver, top_fwd_driver_score = _top_fwd_driver(row)

    regime_text = REGIME_TEXT.get(bucket, "Macro conditions require interpretation.")
    driver_text = DRIVER_TEXT.get(top_driver, top_driver.replace("_", " "))
    validator_text = _validator_summary(row)

    delayed_text = ""
    if top_fwd_driver != "none":
        delayed_text = (
            f" Delayed-impact channels indicate the strongest 45-day forward pressure path is "
            f"{top_fwd_driver.replace('_fwd45d', '').replace('_', ' ')} "
            f"({top_fwd_driver_score:.1f})."
        )

    dispersion_text = ""
    if dispersion > 0:
        dispersion_text = f" Cross-component dispersion currently reads {dispersion:.1f}."

    narrative = (
        f"{regime_text} "
        f"FT-GMI is {score:.1f}, mapped to {bucket}. "
        f"The dominant same-day driver is {driver_text} ({top_driver_score:.1f}). "
        f"Component snapshot — rates: {rates:.1f}, FX: {fx:.1f}, energy: {energy:.1f}, "
        f"equity: {equity:.1f}, credit: {credit:.1f}. "
        f"{validator_text}"
        f"{dispersion_text}"
        f"{delayed_text}"
    )

    return narrative.strip()


def build_macro_narrative():

    df = load_ft_gmi()

    df = build_features(df)

    latest = df.iloc[-1]

    regime = regime_bucket(latest["ft_gmi_score"])

    drivers = f"""
Rates: {latest['rates_stress']:.1f}
FX: {latest['fx_stress']:.1f}
Energy: {latest['energy_stress']:.1f}
Equity: {latest['equity_stress']:.1f}
Credit: {latest['credit_stress']:.1f}
"""

    narrative = narrative_template(
        latest["ft_gmi_score"],
        regime,
        drivers,
    )

    return narrative


def main():
    print(build_macro_narrative())


if __name__ == "__main__":
    main()


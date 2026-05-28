from pathlib import Path
from datetime import datetime, timezone
import json
import hashlib


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")

OUT_DIR = ROOT / "phase3"
OUT_PATH = OUT_DIR / "frozen_signal_definitions.json"


SIGNAL_DEFINITIONS = {
    "liquidity_pressure": "yield_curve_pressure + funding_stress_proxy + credit_spread_pressure",
    "contradiction_severity": "rates_equity_fracture + credit_equity_fracture + liquidity_volatility_fracture",
    "sovereign_pressure": "fx_reserve_pressure + currency_pressure + spread_pressure + external_debt_pressure",
    "narrative_drift": "policy_language_shift + inflation_language_shift + growth_language_shift",
    "confidence_score": "source_quality + data_freshness + signal_agreement + historical_support - contradiction_penalty",
}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    frozen_text = json.dumps(
        SIGNAL_DEFINITIONS,
        sort_keys=True,
    )

    formula_hash = hashlib.sha256(
        frozen_text.encode("utf-8")
    ).hexdigest()

    payload = {
        "system": "IsoVector",
        "module": "frozen-signal-definitions",
        "generated_utc": datetime.now(timezone.utc).isoformat(),

        "signal_freeze_enabled": True,

        "signal_definitions": SIGNAL_DEFINITIONS,

        "signal_definition_count": len(SIGNAL_DEFINITIONS),

        "formula_hash": formula_hash,

        "freeze_objective": (
            "Freeze signal definitions before validation so formulas cannot be silently "
            "changed after reviewing outcomes."
        ),

        "freeze_contract": {
            "formula_hash_required": True,
            "post_outcome_formula_change_forbidden": True,
            "signal_registry_required": True,
            "human_review_required": True,
        },

        "governance": {
            "signal_freeze_governed": True,
            "silent_regime_fitting_blocked": True,
            "llm_writeback_allowed": False,
            "audit_trail_required": True,
        },
    }

    OUT_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote -> {OUT_PATH}")


if __name__ == "__main__":
    main()

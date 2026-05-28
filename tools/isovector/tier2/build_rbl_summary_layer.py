from pathlib import Path
import json
from datetime import datetime

ROOT = Path.cwd()
OUT = ROOT / "data" / "serving" / "isovector" / "tier2"
OUT.mkdir(parents=True, exist_ok=True)

rbl = {
    "artifact": "rbl_institutional_summary_layer",
    "version": "v1",
    "created_at": datetime.now().isoformat(timespec="seconds"),
    "purpose": "Translate deterministic cognition signals into executive-readable interpretation.",
    "rules": {
        "may_interpret_signals": True,
        "may_mutate_zt": False,
        "may_override_deterministic_payloads": False,
        "requires_traceable_signal_basis": True
    },
    "plane_templates": {
        "FX": "FX conditions should be interpreted through liquidity pressure, dollar stress, cross-rate dispersion, & conviction.",
        "RATES": "Rates conditions should be interpreted through curve pressure, policy tension, inflation pressure, & duration stress.",
        "C_FLOW": "Commodity flow conditions should be interpreted through energy pressure, inflation impulse, supply stress, & positioning.",
        "EQUITIES_SECTOR": "Sector conditions should be interpreted through rotation, breadth, concentration, & cyclical/defensive pressure.",
        "EQUITIES_INDUSTRY": "Industry conditions should be interpreted through fragmentation, internal breadth, thematic pressure, & analog risk."
    }
}

(OUT / "rbl_summary_layer_v1.json").write_text(
    json.dumps(rbl, indent=2),
    encoding="utf-8"
)

print("[OK] RBL summary layer built")


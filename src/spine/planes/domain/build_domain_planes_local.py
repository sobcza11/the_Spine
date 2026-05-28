from pathlib import Path
from datetime import datetime, timezone
import json


OUT_DIR = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\planes")


def write_payload(name: str, payload: dict):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{name}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote -> {path}")


def base_payload(plane: str, state: str, signals: list[dict]):
    return {
        "system": "IsoVector",
        "plane": plane,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "signals": signals,
        "governance": {
            "ai_generated": False,
            "writeback_allowed": False,
            "measurement_source": "the_Spine",
            "representation_layer": "IsoVector",
        },
    }


def main():
    write_payload(
        "equities_sector_plane",
        base_payload(
            "equities_sector",
            "selective_rotation",
            [
                {"signal": "sector_rotation", "state": "active", "score": 78},
                {"signal": "internal_breadth", "state": "mixed_improving", "score": 69},
                {"signal": "defensive_cyclical_drift", "state": "cyclical_bias", "score": 73},
            ],
        ),
    )

    write_payload(
        "equities_index_plane",
        base_payload(
            "equities_index",
            "risk_appetite_watch",
            [
                {"signal": "broad_beta_posture", "state": "constructive", "score": 74},
                {"signal": "breadth_participation", "state": "uneven", "score": 63},
                {"signal": "equity_rates_contradiction", "state": "watch", "score": 70},
            ],
        ),
    )

    write_payload(
        "rates_plane",
        base_payload(
            "rates",
            "policy_liquidity_watch",
            [
                {"signal": "yield_curve_cognition", "state": "mixed", "score": 68},
                {"signal": "real_yield_pressure", "state": "tight", "score": 77},
                {"signal": "duration_instability", "state": "watch", "score": 72},
            ],
        ),
    )

    write_payload(
        "fx_plane",
        base_payload(
            "fx",
            "dollar_stress_watch",
            [
                {"signal": "fx_liquidity_posture", "state": "mixed", "score": 66},
                {"signal": "dollar_stress_map", "state": "watch", "score": 71},
                {"signal": "cb_divergence", "state": "active", "score": 75},
            ],
        ),
    )


if __name__ == "__main__":
    main()

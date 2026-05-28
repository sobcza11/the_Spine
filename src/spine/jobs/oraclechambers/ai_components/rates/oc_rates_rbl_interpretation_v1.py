from datetime import datetime, timezone
from typing import Any

from oc_rates_zt_zeitgeist_v1 import build_rates_zt_zeitgeist_v1


def build_rates_rbl_interpretation_v1() -> dict[str, Any]:
    zt = build_rates_zt_zeitgeist_v1()
    rates = zt["zeitgeist"]

    interpretation = (
        "Rates cognition indicates persistent duration pressure, unstable curve "
        "behavior, and restrictive policy conditions. Cross-asset confirmation "
        "remains incomplete, implying that bond-market stress still carries "
        "macro-fragility risk."
    )

    return {
        "artifact": "oc_rates_rbl_interpretation_v1",
        "plane": "RATES",
        "component": "RBL",
        "label": "RBL • Bond Mrkt • Interpretation (OC)",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "ai_component_ready": True,
        "online_transition_allowed": False,
        "source_artifact": zt["artifact"],
        "interpretation": {
            "summary": interpretation,
            "risk_posture": "Defensive duration awareness",
            "decision_bias": (
                "Avoid interpreting isolated equity strength as durable while "
                "rates instability and policy pressure persist."
            ),
            "watch_items": [
                "Yield curve instability",
                "Duration repricing risk",
                "Treasury liquidity stress",
                "Central-bank policy divergence",
            ],
            "confidence": rates["confidence"],
            "conviction": rates["conviction"],
        },
        "governance": {
            "rbl_is_interpretive": True,
            "zt_is_source_signal": True,
            "ai_may_not_mutate_zt": True,
        },
    }


if __name__ == "__main__":
    print(build_rates_rbl_interpretation_v1())

    
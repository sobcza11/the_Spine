from spine.offline_sites.cognition_rules import (
    clean_output,
    compress,
    reserved_space_state,
    sigma_state,
)

from spine.offline_sites.cognition_runtime_config import CONFIG


def build_rates_cognition(
    country,
    curve,
    sigma,
    spread,
):
    z_state = sigma_state(sigma)

    zt = []
    rbl = []
    systemic = []

    if country == "US":
        zt.append(
            "US rates remain the global discount-rate anchor."
        )

        if curve < 1:
            zt.append(
                "Curve structure continues signaling unresolved transition conditions."
            )

        if z_state == "high":
            zt.append(
                "Rates pressure remains elevated relative to the sovereign peer complex."
            )

            systemic.append(
                "US rate pressure can transmit into FX, commodities & duration-sensitive equities."
            )

    elif country == "CA":
        zt.append(
            "Canada remains sensitive to housing, oil & North American credit conditions."
        )

        if curve > 1:
            zt.append(
                "Positive slope reduces acute inversion pressure."
            )

        if z_state == "moderate":
            systemic.append(
                "CAD deterioration alongside housing weakness would increase systemic transmission risk."
            )

    rbl.append(
        f"Spread signal remains at {spread:.2f}."
    )

    return {
        "zt": compress(
            clean_output(zt),
            CONFIG.max_zt_lines,
        ),

        "rbl": compress(
            clean_output(rbl),
            CONFIG.max_rbl_lines,
        ),

        "rbl_systemic": compress(
            clean_output(systemic),
            CONFIG.max_systemic_lines,
        ),

        "reserved_space_decision": reserved_space_state(
            z_state
        ),
    }


def build_fx_cognition(
    pair,
    sigma,
    latest_price,
):
    z_state = sigma_state(sigma)

    zt = []
    rbl = []
    systemic = []

    if pair == "AUDUSD":
        zt.append(
            "AUD strength remains linked to commodity resilience & China-sensitive demand conditions."
        )

        if z_state == "moderate":
            zt.append(
                "FX volatility remains elevated versus normal commodity-linked conditions."
            )

            systemic.append(
                "AUD deterioration could confirm broader global risk-off transmission."
            )

    if pair == "USDJPY":
        zt.append(
            "USD/JPY continues acting as a carry-transmission & funding-stress pair."
        )

        if z_state == "high":
            systemic.append(
                "JPY instability alongside rising US yields increases carry unwind sensitivity."
            )

    rbl.append(
        f"Latest price: {latest_price:.4f}"
    )

    return {
        "zt": compress(
            clean_output(zt),
            CONFIG.max_zt_lines,
        ),

        "rbl": compress(
            clean_output(rbl),
            CONFIG.max_rbl_lines,
        ),

        "rbl_systemic": compress(
            clean_output(systemic),
            CONFIG.max_systemic_lines,
        ),

        "reserved_space_decision": reserved_space_state(
            z_state
        ),
    }

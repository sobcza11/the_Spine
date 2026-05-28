from dataclasses import dataclass


@dataclass
class IVState:
    pressure: float
    fragility: float
    liquidity: float
    dispersion: float
    momentum: float
    cross_asset_stress: float
    coherence: float
    systemicity: float


def classify_state(v):

    if v >= 2:
        return "high"

    if v >= 1:
        return "moderate"

    return "contained"


def build_iv_vector(
    pressure,
    fragility,
    liquidity,
    dispersion,
    momentum,
    cross_asset_stress,
    coherence,
    systemicity,
):

    return IVState(
        pressure=pressure,
        fragility=fragility,
        liquidity=liquidity,
        dispersion=dispersion,
        momentum=momentum,
        cross_asset_stress=cross_asset_stress,
        coherence=coherence,
        systemicity=systemicity,
    )


def summarize_iv_state(iv):

    outputs = []

    if classify_state(iv.pressure) == "high":
        outputs.append(
            "System pressure remains elevated."
        )

    if classify_state(iv.fragility) == "high":
        outputs.append(
            "Structural fragility conditions remain active."
        )

    if classify_state(iv.liquidity) == "high":
        outputs.append(
            "Liquidity stress remains elevated across monitored conditions."
        )

    if classify_state(iv.cross_asset_stress) == "high":
        outputs.append(
            "Cross-asset transmission pressure remains elevated."
        )

    if classify_state(iv.systemicity) == "high":
        outputs.append(
            "Systemic escalation conditions remain active."
        )

    return outputs

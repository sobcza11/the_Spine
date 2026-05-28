from pathlib import Path
import json

from spine.offline_sites.cognition_engine import (
    build_rates_cognition,
    build_fx_cognition,
)

from spine.offline_sites.iv_state_engine import (
    build_iv_vector,
    summarize_iv_state,
)


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine"
)

REVIEW_ROOT = (
    ROOT
    / "data"
    / "review_runtime"
)


def write_payload(
    domain,
    instrument,
    payload,
):

    target = (
        REVIEW_ROOT
        / domain
        / instrument
    )

    target.mkdir(
        parents=True,
        exist_ok=True,
    )

    out = target / "content.json"

    out.write_text(
        json.dumps(
            payload,
            indent=2,
        ),
        encoding="utf-8",
    )


def build_us_rates():

    cognition = build_rates_cognition(
        country="US",
        curve=0.52,
        sigma=-2.01,
        spread=0.52,
    )

    iv = build_iv_vector(
        pressure=2.1,
        fragility=1.2,
        liquidity=1.8,
        dispersion=0.8,
        momentum=1.0,
        cross_asset_stress=2.2,
        coherence=1.7,
        systemicity=2.4,
    )

    cognition["iv_state"] = summarize_iv_state(iv)

    return cognition


def build_audusd():

    cognition = build_fx_cognition(
        pair="AUDUSD",
        sigma=1.23,
        latest_price=0.7117,
    )

    iv = build_iv_vector(
        pressure=1.2,
        fragility=0.7,
        liquidity=0.9,
        dispersion=1.1,
        momentum=1.0,
        cross_asset_stress=1.4,
        coherence=1.6,
        systemicity=1.2,
    )

    cognition["iv_state"] = summarize_iv_state(iv)

    return cognition


def main():

    write_payload(
        domain="rates",
        instrument="US",
        payload=build_us_rates(),
    )

    write_payload(
        domain="fx",
        instrument="AUDUSD",
        payload=build_audusd(),
    )

    print(
        "Offline cognition payloads generated."
    )


if __name__ == "__main__":
    main()
    

from pathlib import Path
import json

from spine.runtime.validation.runtime_health import (
    check_file_health,
)

SERVING_ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\macro\serving"
)

FILES = [
    SERVING_ROOT / "ism_pmi_industry_panel.parquet",
    SERVING_ROOT / "ism_pmi_latest.json",
]


def test_stale_detection():

    checks = []

    for path in FILES:

        result = check_file_health(
            path=path,

            # Temporary Tier 1.5 stabilization threshold
            # Increase later once runtime refresh cadence is live
            max_age_hours=720,
        )

        checks.append(result)

    failures = [
        x for x in checks
        if x["status"] != "ok"
    ]

    print(json.dumps(checks, indent=2))

    assert not failures, (
        f"Stale/missing files detected: {failures}"
    )

from pathlib import Path

from spine.runtime.validation.runtime_health import (
    check_file_health,
    write_runtime_health_report,
)

SERVING_ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\macro\serving"
)

OUT_PATH = SERVING_ROOT / "runtime_health.json"

FILES = [
    SERVING_ROOT / "ism_pmi_industry_panel.parquet",
    SERVING_ROOT / "ism_pmi_latest.json",
]


def main():

    checks = []

    for path in FILES:

        result = check_file_health(
            path=path,
            max_age_hours=720,
        )

        checks.append(result)

    write_runtime_health_report(
        checks=checks,
        out_path=OUT_PATH,
    )

    print(f"Wrote runtime health file -> {OUT_PATH}")


if __name__ == "__main__":
    main()
    
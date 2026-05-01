import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    INDUSTRY_SIGNAL_OUTPUT,
)

REQUIRED_COLUMNS = [
    "date",
    "source",
    "release_type",
    "sector",
    "industry",
    "pmi",
    "new_orders",
    "commentary_count",
    "avg_direction_score",
    "avg_confidence",
    "industry_zt_input",
]


def validate_pmi_industry_signal_v1():
    df = pd.read_parquet(INDUSTRY_SIGNAL_OUTPUT).copy()

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"[FAIL] Missing columns: {missing}")

    if len(df) == 0:
        raise ValueError("[FAIL] No PMI industry signal rows found")

    if df["industry_zt_input"].isna().any():
        raise ValueError("[FAIL] Null industry_zt_input detected")

    print(f"[PASS] PMI industry signal validation passed ({len(df)} rows)")


if __name__ == "__main__":
    validate_pmi_industry_signal_v1()


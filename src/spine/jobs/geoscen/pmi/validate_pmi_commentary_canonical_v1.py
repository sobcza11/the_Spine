import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    CANONICAL_OUTPUT,
    REQUIRED_CANONICAL_COLUMNS,
)


def validate_pmi_commentary_canonical_v1():
    df = pd.read_parquet(CANONICAL_OUTPUT).copy()

    missing = set(REQUIRED_CANONICAL_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"[FAIL] Missing columns: {missing}")

    if len(df) == 0:
        raise ValueError("[FAIL] No PMI commentary rows found")

    for col in ["date", "commentary_text"]:
        if df[col].isna().any():
            raise ValueError(f"[FAIL] Nulls detected in {col}")

    print(f"[PASS] PMI commentary canonical validation passed ({len(df)} rows)")


if __name__ == "__main__":
    validate_pmi_commentary_canonical_v1()

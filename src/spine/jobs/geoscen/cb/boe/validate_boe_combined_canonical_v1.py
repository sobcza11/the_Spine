import pandas as pd

from spine.jobs.geoscen.cb.boe.boe_constants import (
    COMBINED_CANONICAL_OUTPUT,
    BANK_CODE,
    CURRENCY,
)

REQUIRED_COLUMNS = [
    "bank_code",
    "bank_name",
    "currency",
    "language",
    "document_type",
    "title",
    "url",
    "source",
    "ingested_at_utc",
]


def validate_boe_combined_canonical_v1():
    df = pd.read_parquet(COMBINED_CANONICAL_OUTPUT).copy()

    # 1. Column check
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"[FAIL] Missing columns: {missing_cols}")

    # 2. Bank code consistency
    if not (df["bank_code"] == BANK_CODE).all():
        raise ValueError("[FAIL] bank_code mismatch detected")

    # 3. Currency consistency
    if not (df["currency"] == CURRENCY).all():
        raise ValueError("[FAIL] currency mismatch detected")

    # 4. Null checks (critical fields)
    critical_cols = ["title", "url", "document_type"]
    for col in critical_cols:
        if df[col].isna().any():
            raise ValueError(f"[FAIL] Null values detected in {col}")

    # 5. Duplicate check
    dupes = df.duplicated(subset=["bank_code", "document_type", "url"]).sum()
    if dupes > 0:
        raise ValueError(f"[FAIL] Duplicate rows detected: {dupes}")

    # 6. Basic sanity check
    if len(df) == 0:
        raise ValueError("[FAIL] No rows in combined canonical")

    print(f"[PASS] BoE combined canonical validation passed ({len(df)} rows)")


if __name__ == "__main__":
    validate_boe_combined_canonical_v1()


import pandas as pd

from spine.jobs.geoscen.cb.boe.boe_constants import (
    POLICY_MINUTES_OUTPUT,
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


def validate_boe_policy_minutes():
    df = pd.read_parquet(POLICY_MINUTES_OUTPUT).copy()

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
    critical_cols = ["title", "url"]
    for col in critical_cols:
        if df[col].isna().any():
            raise ValueError(f"[FAIL] Null values detected in {col}")

    # 5. Duplicate URL check
    dupes = df.duplicated(subset=["url"]).sum()
    if dupes > 0:
        raise ValueError(f"[FAIL] Duplicate URLs detected: {dupes}")

    # 6. Basic sanity check
    if len(df) == 0:
        raise ValueError("[FAIL] No rows ingested")

    print(f"[PASS] BoE policy minutes validation passed ({len(df)} rows)")


if __name__ == "__main__":
    validate_boe_policy_minutes()


import pandas as pd

from spine.jobs.geoscen.cb.boc.boc_constants import (
    MPR_OUTPUT,
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


def validate_boc_mpr():
    df = pd.read_parquet(MPR_OUTPUT).copy()

    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"[FAIL] Missing columns: {missing_cols}")

    if not (df["bank_code"] == BANK_CODE).all():
        raise ValueError("[FAIL] bank_code mismatch detected")

    if not (df["currency"] == CURRENCY).all():
        raise ValueError("[FAIL] currency mismatch detected")

    for col in ["title", "url", "document_type"]:
        if df[col].isna().any():
            raise ValueError(f"[FAIL] Null values detected in {col}")

    dupes = df.duplicated(subset=["bank_code", "document_type", "url"]).sum()
    if dupes > 0:
        raise ValueError(f"[FAIL] Duplicate rows detected: {dupes}")

    if len(df) == 0:
        raise ValueError("[FAIL] No BoC MPR rows found")

    print(f"[PASS] BoC MPR validation passed ({len(df)} rows)")


if __name__ == "__main__":
    validate_boc_mpr()


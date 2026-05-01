import pandas as pd

from spine.jobs.geoscen.cb.boc.boc_constants import (
    RATE_ANNOUNCEMENTS_OUTPUT,
    MPR_OUTPUT,
    COMBINED_CANONICAL_OUTPUT,
)

CANONICAL_COLUMNS = [
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


def build_boc_combined_canonical_v1():
    df_rates = pd.read_parquet(RATE_ANNOUNCEMENTS_OUTPUT).copy()
    df_mpr = pd.read_parquet(MPR_OUTPUT).copy()

    df = pd.concat([df_rates, df_mpr], ignore_index=True)

    df = df[CANONICAL_COLUMNS].copy()
    df = df.drop_duplicates(subset=["bank_code", "document_type", "url"])
    df = df.sort_values(["document_type", "title"]).reset_index(drop=True)

    COMBINED_CANONICAL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(COMBINED_CANONICAL_OUTPUT, index=False)

    print(f"[OK] Wrote BoC combined canonical ({len(df)} rows)")
    print(f"[PATH] {COMBINED_CANONICAL_OUTPUT}")

    return df


if __name__ == "__main__":
    build_boc_combined_canonical_v1()


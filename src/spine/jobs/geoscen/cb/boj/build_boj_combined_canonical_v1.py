import pandas as pd

from spine.jobs.geoscen.cb.boj.boj_constants import (
    MPM_OUTPUT,
    OUTLOOK_OUTPUT,
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


def build_boj_combined_canonical_v1():
    df_mpm = pd.read_parquet(MPM_OUTPUT).copy()
    df_outlook = pd.read_parquet(OUTLOOK_OUTPUT).copy()

    df = pd.concat([df_mpm, df_outlook], ignore_index=True)

    df = df[CANONICAL_COLUMNS].copy()
    df = df.drop_duplicates(subset=["bank_code", "document_type", "url"])
    df = df.sort_values(["document_type", "title"]).reset_index(drop=True)

    COMBINED_CANONICAL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(COMBINED_CANONICAL_OUTPUT, index=False)

    print(f"[OK] Wrote BoJ combined canonical ({len(df)} rows)")
    print(f"[PATH] {COMBINED_CANONICAL_OUTPUT}")

    return df


if __name__ == "__main__":
    build_boj_combined_canonical_v1()


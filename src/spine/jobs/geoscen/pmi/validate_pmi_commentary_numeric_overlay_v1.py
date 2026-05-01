import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    COMMENTARY_NUMERIC_OVERLAY_OUTPUT,
)

REQUIRED_COLUMNS = [
    "date",
    "source",
    "sector",
    "industry",
    "commentary_text",
    "tag_demand",
    "tag_inflation",
    "tag_supply_chain",
    "direction",
    "confidence",
    "numeric_relevance_flag",
]


def validate_pmi_commentary_numeric_overlay_v1():
    df = pd.read_parquet(COMMENTARY_NUMERIC_OVERLAY_OUTPUT).copy()

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"[FAIL] Missing columns: {missing}")

    if len(df) == 0:
        raise ValueError("[FAIL] No overlay rows found")

    if df["commentary_text"].isna().any():
        raise ValueError("[FAIL] Null commentary_text detected")

    if not df["numeric_relevance_flag"].isin([0, 1]).all():
        raise ValueError("[FAIL] numeric_relevance_flag must be 0/1")

    print(f"[PASS] PMI commentary numeric overlay validation passed ({len(df)} rows)")


if __name__ == "__main__":
    validate_pmi_commentary_numeric_overlay_v1()

    
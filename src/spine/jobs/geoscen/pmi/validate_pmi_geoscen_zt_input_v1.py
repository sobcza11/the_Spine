import pandas as pd

from spine.jobs.geoscen.pmi.pmi_commentary_constants import (
    GEOSCEN_ZT_INPUT_OUTPUT,
)

REQUIRED_COLUMNS = [
    "date",
    "sector",
    "industry",
    "commentary_count",
    "avg_confidence",
    "avg_text_signal_score",
    "avg_numeric_signal_score",
    "pmi_geoscen_zt_input",
]


def validate_pmi_geoscen_zt_input_v1():
    df = pd.read_parquet(GEOSCEN_ZT_INPUT_OUTPUT).copy()

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"[FAIL] Missing columns: {missing}")

    if len(df) == 0:
        raise ValueError("[FAIL] No PMI GeoScen Zt rows found")

    if df["pmi_geoscen_zt_input"].isna().any():
        raise ValueError("[FAIL] Null pmi_geoscen_zt_input detected")

    print(f"[PASS] PMI GeoScen Zt input validation passed ({len(df)} rows)")


if __name__ == "__main__":
    validate_pmi_geoscen_zt_input_v1()

